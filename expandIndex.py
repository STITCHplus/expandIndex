#!/usr/bin/env python3

##
## expandIndex.py - expandIndex
##
## copyright (c) 2011-2012 Koninklijke Bibliotheek - National library of the Netherlands.
##
## this program is free software: you can redistribute it and/or modify
## it under the terms of the gnu general public license as published by
## the free software foundation, either version 3 of the license, or
## (at your option) any later version.
##
## this program is distributed in the hope that it will be useful,
## but without any warranty; without even the implied warranty of
## merchantability or fitness for a particular purpose. see the
## gnu general public license for more details.
##
## you should have received a copy of the gnu general public license
## along with this program. if not, see <http://www.gnu.org/licenses/>.
##

import gzip,os,string,sys,time,random,unicodedata
import time, urllib.request, urllib.parse, ast
import http.client

from pprint import pprint
from xml.etree import ElementTree as etree

import urllib

__author__ = "Willem Jan Faber"

def post_url(data):
    try:
        headers = {"Content-type" : "text/xml; charset=utf-8", "Accept": "text/plain"}
        conn = http.client.HTTPConnection("kbresearch.nl:80")
        conn.request("POST","/solr/ggc-expand/update/", bytes(data.encode('utf-8')), headers)
        response = conn.getresponse()
        res = response.read()
        if not str(res).find("<int name=\"status\">0</int>") > -1:
            print(res)
        conn.close()
    except:
        time.sleep(10)
    return()

class OaiHarvest(object):
    baseurl = ""
    expandurl = ""
    expandparam = ""
    unwanted_tags = "responseDate","request","datestamp"

    def __init__(self, baseurl, expandurl, expandparam):
        self.baseurl = baseurl
        self.expandurl = expandurl
        self.expandparam = expandparam

    def _get(self, url, xml_parse=False):
        return(urllib.request.urlopen(str(url)).read().decode("utf-8"))

    def listIdentifiers(self, setname, token):
        identifier=[]
        resumptiontoken=False

        if (type(token) != type(True)):
            url=self.baseurl+"?verb=ListIdentifiers&set=GGC&metadataPrefix=INDEXING&resumptionToken="+token
        else:
            url=self.baseurl+"?verb=ListIdentifiers&set=GGC&metadataPrefix=INDEXING"
        
        print(url)

        try:
            record=self._get(url)
        except:
            os._exit(-1)

        for line in record.split("<"):
            if line.find(">") > -1:
                if line.find(":") > -1:
                    if line.lower().startswith("identifier>"):
                        identifier.append(line.split(">")[1].strip())
                    if line.lower().startswith("resumptiontoken>"):
                        resumptiontoken=line.split(">")[1].strip()
        print(resumptiontoken, identifier)
        return(resumptiontoken, identifier)


    def solr_query(self):
        pass

    def get_record_count(self, identifier):
        url = ("http://www.kbresearch.nl/solr/ggc/?q=\"%s\"&wt=json" %(identifier))
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        data = ast.literal_eval(response.read().decode('utf-8'))
        try:
            if (data["response"]["docs"][0]["id"] == identifier):
                return(True)
            else:
                return(False)
        except:
            return(False)

    def getRecord(self, identifier):
        data=[]
        count=0
        record=etree.XML(self._get(self.baseurl+"?verb=GetRecord&metadataPrefix=INDEXING&identifier="+identifier, True))
        doc=etree.Element("doc")
        identifier=False
        for item in record.getiterator():
            if item.tag.split("}")[1] not in self.unwanted_tags:
                if item.text:
                    if len(str(item.text).strip()) > 0:
                        if (item.tag.split("}")[1] == "PPN"):

                            add=etree.SubElement(doc, 'field', {"name" : "PPN" })
                            add.text=item.text

                            add=etree.SubElement(doc, 'field', {"name" : "PPN_str" })
                            add.text=item.text

                            data = urllib.parse.urlencode(self.expandparam)
                            data+='&q="GGC-THES:'+item.text+'"%20AND%20(type:person%20OR%20type:persoon)'

                            data+="&facet.field=altLabel_str&facet.field=prefLabel&facet.method=enum"

                            expand_names={}
                            print(self.expandurl,data)
                            req = urllib.request.Request(self.expandurl, data)
                            response = urllib.request.urlopen(req)

                            data = ast.literal_eval(response.read().decode('utf-8'))
                            expand_names={}
                            if data["response"]["numFound"] > 0:
                                for key in data["facet_counts"]["facet_fields"].keys():
                                    if key.endswith("_str"):
                                        for name in data["facet_counts"]["facet_fields"][key]:
                                            if type(name) != type(1):
                                                if not name.find('(vorm)') > -1:
                                                            expand_names[name]=True

                            for name in expand_names:
                                add=etree.SubElement(doc, 'field', {"name" : "altCreator_str" })
                                add.text=name
                                add=etree.SubElement(doc, 'field', {"name" : "altCreator" })
                                add.text=name
                                print(name)
                                count+=1
                        else:    
                            if not identifier:
                                add=etree.SubElement(doc, 'field', {"name" : item.tag.split("}")[1].replace("identifier","id") })
                                add.text=item.text
                                add=etree.SubElement(doc, 'field', {"name" : item.tag.split("}")[1].replace("identifier","id")+"_str" })
                                add.text=item.text
                                print(item.text)
                                identifier=item.text
                            else:
                                add=etree.SubElement(doc, 'field', {"name" : item.tag.split("}")[1] })
                                add.text=item.text
                                add=etree.SubElement(doc, 'field', {"name" : item.tag.split("}")[1]+"_str" })
                                add.text=item.text
        return(etree.tostring(doc), count)

OAI_BASEURL     = "http://services.kb.nl/mdo/oai"
OAI_DEV_BASEURL = "http://serviceso.kb.nl/mdo/oai"
EXPAND_URL      = "http://www.kbresearch.nl/solr/ggc-thes"
EXPAND_PARAM    = { 'facet.mincount'  : 1,
                    'rows'            : 0,
                    'facet'           : 'true',
                    'wt'              : 'json' }



if __name__ == "__main__":
    oaiharvester = OaiHarvest(OAI_BASEURL, EXPAND_URL, EXPAND_PARAM)
    token=True

    while(token):
        fail=False
        last_token=token
        (token, identifiers) = oaiharvester.listIdentifiers("GGC", token)
        name=[]

        if len(identifiers) > 0:
            add="<add>"
            count=exrate=0
            for identifier in identifiers:
                (data,ex)=oaiharvester.getRecord(identifier)
                exrate+=ex
                add+=data
                count+=1
                print ("Working on record : %3i / %3i \t next_token : %s\t current_token : %s" %(count, len(identifiers), token, last_token))
                print ("Lenth data : %i, names expanded %i" %(len(add), exrate))
            add+="</add>"
            print(add)
            post_url(add)
            os._exit(-1)
