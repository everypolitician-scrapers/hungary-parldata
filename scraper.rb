#!/bin/env ruby
# encoding: utf-8

require 'scraperwiki'
require 'nokogiri'
require 'open-uri'
require 'colorize'
require 'rest-client'
require 'csv'

require 'pry'
require 'open-uri/cached'
OpenURI::Cache.cache_path = '.cache'

@API_URL = 'http://api.parldata.eu/hu/orszaggyules/%s'

termdates = <<EODATA
id,start_date,end_date
34,1990-05-02,1994-06-27
35,1994-06-28,1998-06-17
36,1998-06-18,2002-05-14
37,2002-05-15,2006-05-15
38,2006-05-16,2010-05-13
39,2010-05-14,2014-05-05
40,2014-05-06,
EODATA
@termdata = CSV.parse(termdates, headers: true, header_converters: :symbol)

def noko_q(endpoint, h)
  result = RestClient.get (@API_URL % endpoint), params: h
  doc = Nokogiri::XML(result)
  doc.remove_namespaces!
  entries = doc.xpath('resource/resource')
  return entries if (np = doc.xpath('.//link[@rel="next"]/@href')).empty?
  return [entries, noko_q(endpoint, h.merge(page: np.text[/page=(\d+)/, 1]))].flatten
end

def overlap(mem, term)
  mS = mem[:start_date].to_s.empty?  ? '0000-00-00' : mem[:start_date]
  mE = mem[:end_date].to_s.empty?    ? '9999-99-99' : mem[:end_date]
  tS = term[:start_date].to_s.empty? ? '0000-00-00' : term[:start_date]
  tE = term[:end_date].to_s.empty?   ? '9999-99-99' : term[:end_date]

  return unless mS < tE && mE > tS
  (s, e) = [mS, mE, tS, tE].sort[1,2]
  return { 
    start_date: s == '0000-00-00' ? nil : s,
    end_date:   e == '9999-99-99' ? nil : e,
  }
end

# http://api.parldata.eu/hu/orszaggyules/organizations?where={"classification":"chamber"}
xml = noko_q('organizations', where: %Q[{"classification":"chamber"}] )
xml.each do |chamber|
  term = { 
    id: chamber.xpath('.//identifiers[scheme[text()="parlament.hu/chamber"]]/identifier').text,
    identifier__parldata: chamber.xpath('.//id').text,
    name: chamber.xpath('.//name').text.sub('Kuvendit të Kosovës - ',''),
  }
  row = @termdata.find { |r| r[:id] == term[:id] } or raise "Unknown term #{term[:id]}"
  term[:start_date] = row[:start_date]
  term[:end_date] = row[:end_date]
  ScraperWiki.save_sqlite([:id], term, 'terms')

  # http://api.parldata.eu/hu/orszaggyules/memberships?where={"organization_id":"550303bc273a39033bab34e1"}&embed=["person.memberships.organization"]
  mems = noko_q('memberships', { 
    where: %Q[{"organization_id":"#{term[:identifier__parldata]}"}],
    max_results: 50,
    embed: '["person.memberships.organization"]'
  })

  mems.each do |mem|
    person = mem.xpath('person')
    person.xpath('changes').each { |m| m.remove } # make eyeballing easier
    data = { 
      id: person.xpath('.//identifiers[scheme[text()="parlament.hu/people"]]/identifier').text,
      identifier__parldata: person.xpath('id').text,
      name: person.xpath('name').text,
      sort_name: person.xpath('sort_name').text,
      family_name: person.xpath('family_name').text,
      birth_date: person.xpath('birth_date').text,
      honorific_prefix: person.xpath('honorific_prefix').text,
      email: person.xpath('email').text,
      image: person.xpath('image').text,
      source: person.xpath('sources/url').first.text,
      term: term[:id],
    }
    data.delete :sort_name if data[:sort_name] == ','

    mems = person.xpath('memberships[organization[classification[text()="party"]]]').map { |m|
      {
        party: m.xpath('organization/name').text,
        party_id: m.xpath('.//identifiers[scheme[text()="parlament.hu/parties"]]/identifier').text,
        start_date: m.xpath('start_date').text,
        end_date: m.xpath('end_date').text,
      }
    }.select { |m| overlap(m, term) } 

    if mems.count.zero?
      row = data.merge({
        party: 'Unknown', # or none?
        party_id: '_unknown',
      })
      ScraperWiki.save_sqlite([:id, :term], row)
    else
      mems.each do |mem|
        range = overlap(mem, term) or raise "No overlap"
        row = data.merge(mem).merge(range)
        ScraperWiki.save_sqlite([:id, :term, :start_date], row)
      end
    end
  end
end

