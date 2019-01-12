#!/usr/bin/env ruby
# coding: utf-8

STDOUT.sync = true

require 'linkeddata'
require 'date'
require 'securerandom'
require 'tempfile'
require 'csv'
require 'pry-byebug'

class DocsDeleter
  attr_reader :client, :log

  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
  PERSOON = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/persoon#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  BASE_IRI='http://data.lblod.info/id'

  DOCSTATES = { "besluitenlijst publiek" => "http://mu.semte.ch/application/editor-document-statuses/b763390a63d548bb977fb4804293084a",
                "prullenbak" => "http://mu.semte.ch/application/editor-document-statuses/5A8304E8C093B00009000010",
                "agenda publiek" => "http://mu.semte.ch/application/editor-document-statuses/627aec5d144c422bbd1077022c9b45d1"}

  def initialize(endpoint)
    @endpoint = endpoint
    @client = SPARQL::Client.new(endpoint)
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
    wait_for_db
    @manual_check = []
  end

  def generate_query()
    graphs = find_graphs_with_doc()
    all_docs_to_delete = []
    graphs.each do |graph|
      triples = find_published_docs(graph.g.value)
      if triples.length <= 1
        p "No duplicates found for #{graph.g.value}"
        next
      end
      triples_to_d = filter_duplicate_docs_from_sorted_triples(triples)
      all_docs_to_delete += triples_to_d
    end
    p "Moving #{all_docs_to_delete.length} documents to prullenbak"
    generate_move_status(all_docs_to_delete)
    print_things_to_check_manually
  end

  def find_graphs_with_doc()
    query(%(
          PREFIX pav: <http://purl.org/pav/>
          PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

          SELECT DISTINCT ?g
          WHERE {
            GRAPH ?g {
              ?s a ext:EditorDocument.
           }
         }
       ))
  end

  def find_published_docs(eenheid_g)
    uuid = eenheid_g.gsub("http://mu.semte.ch/graphs/organizations/", "")
    query_str = %(
            PREFIX pav: <http://purl.org/pav/>
            PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
            PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

            SELECT DISTINCT ?doc ?modified ?eenheidType ?eenheidNaam ?statusName ?status
            WHERE {
              GRAPH <http://mu.semte.ch/graphs/public> {
                ?eenheid mu:uuid "#{uuid}".
                ?eenheid skos:prefLabel ?eenheidNaam.
                ?eenheid besluit:classificatie ?classS.
                ?classS skos:prefLabel ?eenheidType.
                ?status ext:EditorDocumentStatusName ?statusName
               }

              GRAPH <#{eenheid_g}> {
                ?doc a ext:EditorDocument.
                ?doc ext:editorDocumentStatus ?status.
                ?doc pav:lastUpdateOn ?modified.
                FILTER(
                  NOT EXISTS {
                       ?prevV pav:previousVersion ?doc.
                  }
                )
                FILTER (?status in (<#{DOCSTATES["agenda publiek"]}>, <#{DOCSTATES["besluitenlijst publiek"]}>))
              }
            }
            ORDER BY ?modified
         )
    query(query_str)
  end

  def filter_duplicate_docs_from_sorted_triples(triples)
    ########################################################################
    # Assumes triples are sorted by date
    ########################################################################
    inverted_mapping = DOCSTATES.invert

    if(not triples.length == (triples.uniq{ |t| t.doc.value }).length)
      raise "duplicate doc uri found. Sure query correct?"
    end

    # last document modified is besluitenlijst publiek remove other docs
    if  inverted_mapping[triples[-1].status.value] == "besluitenlijst publiek"
      p "Last entry (besluiten) is valid for #{triples[-1].eenheidNaam.value}"
      triples.pop()
      return triples
    end

    has_besluitenlijst = triples.find{ |t| inverted_mapping[t.status.value] == "besluitenlijst publiek" }

    if  inverted_mapping[triples[-1].status.value] == "agenda publiek" and not has_besluitenlijst
      p "Last entry (agenda) is valid for #{triples[-1].eenheidNaam.value}"
      triples.pop()
      return triples
    end

    #Here we arrive in some weird state better see that is happening
    @manual_check << triples[-1]
    []
  end

  def generate_move_status(docs)
    inverted_mapping = DOCSTATES.invert
    # filter agenda en besluiten
    agenda = docs.select{ |t| inverted_mapping[t.status.value] == "agenda publiek"}
    besluiten = docs.select{ |t| inverted_mapping[t.status.value] == "besluitenlijst publiek"}

    agenda_uris = agenda.map{ |u| "<#{u}>"}.join(",")
    besluiten_uris = besluiten.map{ |u| "<#{u}>"}.join(",")

    query = %(
      PREFIX ns5:  <http://purl.org/dc/terms/>
      PREFIX ns2: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

       DELETE {
         GRAPH ?g {
           ?s ext:editorDocumentStatus <#{DOCSTATES["agenda publiek"]}>.
         }
       }

       INSERT {
         GRAPH ?g {
           ?s ext:editorDocumentStatus <#{DOCSTATES["prullenbak"]}>.
         }
       }

       WHERE {
         GRAPH ?g {
           ?s ?p ?o .
           FILTER( ?s IN (#{agenda_uris})) .
         }
       };

       DELETE {
         GRAPH ?g {
           ?s ext:editorDocumentStatus <#{DOCSTATES["besluitenlijst publiek"]}>.
         }
       }

       INSERT {
         GRAPH ?g {
           ?s ext:editorDocumentStatus <#{DOCSTATES["prullenbak"]}>.
         }
       }

       WHERE {
         GRAPH ?g {
           ?s ?p ?o .
           FILTER( ?s IN (#{besluiten_uris})) .
         }
       };
    )

    file_path = File.join(ENV['OUTPUT_PATH'],"#{DateTime.now.strftime("%Y%m%d%H%M%S")}-remove-duplicates.sparql")
    open(file_path, 'w') { |f| f << query }
    query
  end

  def print_things_to_check_manually
    p "!!!!!!! Some weird state where agenda has been modified after besluitenlijst publiek for:"
    @manual_check.each do |t|
      p "- EENHEID: #{t.eenheidNaam.value}, TYPE #{t.eenheidType.value}"
    end
  end


  # def write_ttl_to_file(name)
  #   output = Tempfile.new(name)
  #   begin
  #     output.write "# started #{name} at #{DateTime.now}"
  #     yield output
  #     output.write "# finished #{name} at #{DateTime.now}"
  #     output.close
  #     FileUtils.copy(output, File.join(ENV['OUTPUT_PATH'],"#{DateTime.now.strftime("%Y%m%d%H%M%S")}-#{name}.ttl"))
  #     output.unlink
  #   rescue StandardError => e
  #     puts e
  #     puts e.backtrace
  #     puts "failed to successfully write #{name}"
  #     output.close
  #     output.unlink
  #   end
  # end
  # def csv_parse_options
  #   { headers: :first_row, return_headers: true, encoding: 'UTF-8' }
  # end

  # def read_csv(file)
  #   headers_parsed = false
  #   index = 0
  #   begin
  #     ::CSV.foreach(file, csv_parse_options) do |row|
  #       unless headers_parsed
  #         @columnCount = row.size
  #         headers_parsed = true
  #         next
  #       end
  #       yield(index, row)
  #       index += 1
  #     end
  #   rescue ::CSV::MalformedCSVError => e
  #     log.error e.message
  #     log.error "parsing stopped after this error on index #{index}"
  #   end
  # end
  def query(q)
    log.debug q
    @client.query(q)
  end
  #   def bestuursorgaan_voor_gemeentenaam(naam, type, date)
  #   @bestuursorgaan_cache ||= {}
  #   orgaan = @bestuursorgaan_cache.dig(naam, type, date)
  #   if orgaan
  #     return orgaan
  #   end
  #   r = query(%(
  #         PREFIX org: <http://www.w3.org/ns/org#>
  #         PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
  #         PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  #         SELECT ?iri
  #         WHERE {
  #            ?iri a besluit:Bestuursorgaan ;
  #                 mandaat:isTijdspecialisatieVan ?orgaan;
  #                 mandaat:bindingStart "#{date}"^^<http://www.w3.org/2001/XMLSchema#date>.
  #            ?eenheid a besluit:Bestuurseenheid ;
  #                     skos:prefLabel "#{naam}".
  #            ?orgaan besluit:bestuurt ?eenheid;
  #                    besluit:classificatie <#{type}>. # will at some point become org:classification
  #         }
  #  ))
  #   if r.size == 0
  #     raise "geen bestuursorgaan gevonden voor #{naam}!"
  #   end
  #   if r.size > 1
  #     raise "meerdere bestuursorganen gevonden voor #{naam}!"
  #   end
  #   @bestuursorgaan_cache[naam] ||= {}
  #   @bestuursorgaan_cache[naam][type] ||= {}
  #   @bestuursorgaan_cache[naam][type][date] = r[0][:iri]
  #   @bestuursorgaan_cache[naam][type][date]
  # end
  # def find_person(rrn)
  #   result = query(%(
  #     PREFIX adms:<http://www.w3.org/ns/adms#>
  #     PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
  #     SELECT ?person ?identifier
  #     WHERE {
  #       ?person adms:identifier ?identifier.
  #       ?identifier skos:notation "#{rrn}".
  #     }

  #   ))
  #   if result.size > 0
  #     [result[0][:person], result[0][:identifier]]
  #   else
  #     raise "person with rrn #{rrn} not found"
  #   end
  # end
  # def find_mandaat(orgaan, type)
  #   result = query(%(
  #         PREFIX org: <http://www.w3.org/ns/org#>
  #         PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  #   SELECT ?mandaat WHERE {
  #              <#{orgaan.to_s}> org:hasPost ?mandaat.
  #              ?mandaat org:role <#{type}>.
  #   }))
  #   if result.size > 0
  #     log.debug result[0][:mandaat].inspect
  #     result[0][:mandaat]
  #   else
  #     raise "wow, no mandaat found for #{orgaan}"
  #   end
  # end

  # def _pure_updated_info(key, existing_info, new_info)
  #   existing_info.key?(key) and new_info[key] and new_info[key][:value].to_s != existing_info[key].value
  # end

  # def _removed_info(key, existing_info, new_info)
  #   existing_info.key?(key) and new_info.key?(key) == false
  # end

  # def update_mandataris(mandataris, existing_info, new_info)
  #   graph = RDF::Repository.new

  #   # needs update
  #   updated_data = new_info.each.select { |key, value|  _pure_updated_info(key, existing_info, new_info)}
  #   if not updated_data.empty?
  #     res = find_mandataris_info_uri(mandataris)
  #     # update all info
  #     new_info.each do |key, value|
  #       if _pure_updated_info(key, existing_info, new_info)
  #         puts "--- Mandataris #{mandataris} has updated info"
  #         puts "--- Found updated info #{key} #{new_info[key][:value].to_s} vs. #{existing_info[key].value}"
  #       end
  #       graph << [ mandataris, new_info[key][:iri], new_info[key][:value]]
  #     end

  #     update_mandataris_minimal(graph, mandataris, res)
  #     @mandatarissen_to_delete << mandataris
  #     return graph
  #   end

  #   # removed info
  #   removed_data = [:datumMinistrieelBesluit, :start, :datumEedaflegging].each.select { |key, value|  _removed_info(key, existing_info, new_info)}
  #   if not removed_data.empty?
  #     # update all info
  #     res = find_mandataris_info_uri(mandataris)
  #     new_info.each do |key, value|
  #       if _removed_info(key, existing_info, new_info)
  #         puts "--- Mandataris #{mandataris} has removed info"
  #       end
  #       graph << [ mandataris, new_info[key][:iri], new_info[key][:value]]
  #     end
  #     update_mandataris_minimal(graph, mandataris, res)
  #     @mandatarissen_to_delete << mandataris
  #     return graph
  #   end

  #   # insert new triples
  #   new_info.each do |key, value|
  #       if (not existing_info.key?(key)) and new_info[key]
  #       graph << [ mandataris, new_info[key][:iri], new_info[key][:value]]
  #     end
  #   end
  #   graph
  # end

  # def generate_mandatarissen_to_delete_query()
  #   uris_to_flush = @mandatarissen_to_delete.map{ |u| "<#{u}>"}.join(",")

  #   query = %(
  #              PREFIX ns5:  <http://purl.org/dc/terms/>
  #              PREFIX ns2: <http://mu.semte.ch/vocabularies/core/>
  #              # delete subscenario
  #               DELETE {
  #                 GRAPH ?g {
  #                   ?s ?p ?o.
  #                 }
  #               }
  #               WHERE {
  #                 GRAPH ?g {
  #                   ?s ?p ?o .
  #                   FILTER( ?s IN (#{uris_to_flush})) .
  #                 }
  #               };
  #          )

  #   file_path = File.join(ENV['OUTPUT_PATH'],"#{@timestamp_delete_mandatarissen_file}-remove-burgemeesters.sparql")
  #   open(file_path, 'w') { |f| f << query }

  #   query
  # end

  # def create_mandataris(persoon, mandaat, status, datum_eed = nil, datum_start = nil, datum_besluit = nil)
  #   graph = RDF::Repository.new
  #   uuid = SecureRandom.uuid
  #   iri = RDF::URI.new("#{BASE_IRI}/mandatarissen/#{uuid}")
  #   graph << [ iri , RDF.type, MANDAAT.Mandataris ]
  #   graph << [ iri, MU.uuid, uuid ]
  #   graph << [ iri, ORG.holds, mandaat ]
  #   graph << [ iri, EXT.datumEedaflegging, Date.strptime(datum_eed, "%m/%d/%Y")]
  #   if datum_start
  #     graph << [ iri, MANDAAT.start, Date.strptime(datum_start, "%m/%d/%Y")]
  #   else
  #     graph << [ iri, MANDAAT.start, Date.strptime("1/1/2019", "%m/%d/%Y")] # we can default to this according to V
  #   end
  #   if datum_besluit
  #     graph << [ iri, EXT.datumMinistrieelBesluit, Date.strptime(datum_besluit, "%m/%d/%Y")]
  #   end
  #   graph << [ iri, MANDAAT.isBestuurlijkeAliasVan, persoon]
  #   graph << [ iri, MANDAAT.status, status]
  #   [graph, iri]
  # end

  # def find_mandataris_info_uri(uri)
  #   result = query(%(
  #       PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  #       PREFIX org: <http://www.w3.org/ns/org#>
  #       PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>

  #       SELECT ?uuid, ?mandaat, ?persoon, ?status WHERE{
  #         <#{uri.to_s}> mu:uuid ?uuid.
  #         <#{uri.to_s}> mandaat:isBestuurlijkeAliasVan ?persoon.
  #         <#{uri.to_s}> mandaat:status ?status.
  #         <#{uri.to_s}> org:holds ?mandaat.
  #        }
  #    ))
  #   if result.size == 1
  #     return result[0]
  #   end
  # end

  # def update_mandataris_minimal(graph, mandataris, data)
  #   graph << [ mandataris , RDF.type, MANDAAT.Mandataris ]
  #   graph << [ mandataris, MU.uuid, data.uuid ]
  #   graph << [ mandataris, ORG.holds, data.mandaat ]
  #   graph << [ mandataris, MANDAAT.isBestuurlijkeAliasVan, data.persoon]
  #   graph << [ mandataris, MANDAAT.status, data.status]
  # end

  # def find_mandataris(orgaan, type)
  #   result = query(%(
  #         PREFIX org: <http://www.w3.org/ns/org#>
  #         PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  #         PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  #         PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
  #          SELECT ?mandataris ?start ?datumEedaflegging ?datumMinistrieelBesluit ?status {
  #              <#{orgaan.to_s}> org:hasPost ?mandaat.
  #              ?mandaat org:role <#{type}>.
  #              ?mandataris org:holds ?mandaat.
  #              OPTIONAL { ?mandataris ext:datumEedaflegging ?datumEedaflegging}
  #              OPTIONAL { ?mandataris mandaat:start ?start}
  #              OPTIONAL { ?mandataris mandaat:status ?status}
  #              OPTIONAL { ?mandataris ext:datumMinistrieelBesluit ?datumMinistrieelBesluit}
  #   }))
  #   if result.size == 1
  #     return result[0]
  #   else
  #     log.error result.inspect
  #     raise "number of mandatarissen found is not 1"
  #   end
  # end

  # def mandataris_exists(orgaan, type, rrn)
  #   query(%(
  #         PREFIX org: <http://www.w3.org/ns/org#>
  #         PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  #         PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
  #         PREFIX adms: <http://www.w3.org/ns/adms#>
  #          ASK {
  #              <#{orgaan.to_s}> org:hasPost ?mandaat.
  #              ?mandaat org:role <#{type}>.
  #              ?mandataris org:holds ?mandaat.
  #              ?mandataris mandaat:isBestuurlijkeAliasVan ?persoon.
  #              ?persoon adms:identifier ?id.
  #              ?id <http://www.w3.org/2004/02/skos/core#notation> "#{rrn}".
  #   }))
  # end
  def wait_for_db
    until is_database_up?
      log.info "Waiting for database... "
      sleep 2
    end

    log.info "Database is up"
  end

  def is_database_up?
    begin
      location = URI(@endpoint)
      response = Net::HTTP.get_response( location )
      return response.is_a? Net::HTTPSuccess
    rescue Errno::ECONNREFUSED
      return false
    end
  end

  # def remove_if_needed(row, orgaantype, burgemeesterRole, mandataris_statussen)
  #   if !row["te verwijderen"]
  #     return false
  #   end
  #   gemeentenaam = row["kieskring"]
  #   datum_eed = row["datum eedaflegging"]
  #   datum_besluit= row["datum besluit"]
  #   datum_start = row["Datum start mandaat"]
  #   rol = row["Mandaat"]

  #   orgaan = bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, "2019-01-01" )
  #   status = mandataris_statussen[rol.downcase]

  #   if mandataris_exists(orgaan, burgemeesterRole, row["RR"])
  #     result = find_mandataris(orgaan, burgemeesterRole)
  #     p "-- mandataris #{result['mandataris']} in delete row #{row}"
  #     @mandatarissen_to_delete << result['mandataris']
  #   end

  #   return true

  # end

  # def append_mandatarissen_to_delete(mandataris)
  #   @mandatarissen_to_delete << mandataris
  # end

end


mdb = DocsDeleter.new(ENV['ENDPOINT'])
mdb.generate_query()
# orgaantype=RDF::URI.new('http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/4955bd72cd0e4eb895fdbfab08da0284') # burgemeester
# burgemeesterRole="http://data.vlaanderen.be/id/concept/BestuursfunctieCode/5ab0e9b8a3b2ca7c5e000013"
# mandataris_statussen = {
#   "burgemeester" =>  RDF::URI.new("http://data.vlaanderen.be/id/concept/MandatarisStatusCode/21063a5b-912c-4241-841c-cc7fb3c73e75"),
#   "waarnemend burgemeester" => RDF::URI.new("http://data.vlaanderen.be/id/concept/MandatarisStatusCode/e1ca6edd-55e1-4288-92a5-53f4cf71946a"),
#   "titel voerend burgemeester" => RDF::URI.new("http://data.vlaanderen.be/id/concept/MandatarisStatusCode/aacb3fed-b51d-4e0b-a411-f3fa641da1b3")

# }
# mdb.write_ttl_to_file("burgemeesters") do |file|
#   mdb.read_csv(File.join(ENV['INPUT_PATH'],'burgemeesters2019.csv')) do |index, row|
#     begin
#       gemeentenaam = row["kieskring"]
#       datum_eed = row["datum eedaflegging"]
#       datum_besluit= row["datum besluit"]
#       datum_start = row["Datum start mandaat"]
#       rol = row["Mandaat"]

#       #manage the remove rows
#       if(mdb.remove_if_needed(row, orgaantype, burgemeesterRole, mandataris_statussen))
#         next
#       end

#       if rol and mandataris_statussen.keys.include?(rol.downcase)
#         orgaan = mdb.bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, "2019-01-01" )
#         status = mandataris_statussen[rol.downcase]
#         if mdb.mandataris_exists(orgaan, burgemeesterRole, row["RR"])

#           puts "updating burgemeester voor #{gemeentenaam} (additions only)"
#           result = mdb.find_mandataris(orgaan, burgemeesterRole)


#           if datum_eed.nil? || datum_eed.empty?
#             p "--- Datum eed has been removed -> remove mandataris #{result['mandataris']}"
#             mdb.append_mandatarissen_to_delete(result['mandataris'])
#             next
#           end

#           new_info = {}
#           if datum_eed
#             new_info[:datumEedaflegging] = {iri: MandatenDb::EXT.datumEedaflegging , value: Date.strptime(datum_eed, "%m/%d/%Y")}
#           end
#           if datum_besluit
#             new_info[:datumMinistrieelBesluit]= {iri: MandatenDb::EXT.datumMinistrieelBesluit , value: Date.strptime(datum_besluit, "%m/%d/%Y")}
#           end
#           if datum_start
#             new_info[:start]= {iri: MandatenDb::MANDAAT.start , value: Date.strptime(datum_start, "%m/%d/%Y")}
#           else
#             new_info[:start]= {iri: MandatenDb::MANDAAT.start , value: Date.strptime("1/1/2019", "%m/%d/%Y")}
#           end
#           graph = mdb.update_mandataris(result["mandataris"], result.to_h, new_info)
#           file.write graph.dump(:ttl)
#         else
#           if not (datum_eed.nil? || datum_eed.empty?)
#             puts "creating burgemeester voor #{gemeentenaam}"
#             (persoon, identifier) = mdb.find_person(row['RR'])
#             burgemeester = mdb.find_mandaat(orgaan, burgemeesterRole)
#             (mandataris, iri) = mdb.create_mandataris(persoon, burgemeester, status, datum_eed, datum_start, datum_besluit)
#             file.write mandataris.dump(:ttl)
#           end
#         end
#       end
#     rescue StandardError => e
#       puts e
#     end
#   end
# end
# mdb.generate_mandatarissen_to_delete_query()
