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

  def query(q)
    log.debug q
    @client.query(q)
  end

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

end


mdb = DocsDeleter.new(ENV['ENDPOINT'])
mdb.generate_query()
