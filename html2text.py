#!/usr/bin/python2.5
"""
File: html2text.py

Copyright (C) 2008  Chris Spencer (chrisspen at gmail dot com)

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
"""

import os, sys, htmllib, formatter, StringIO, re, HTMLParser, htmlentitydefs
import socket, httplib, urllib2, time
try:
    import tidy
except ImportError, e:
    print "You need to install the Python wrapper for TidyLib."
    raise

def unescapeHTMLEntities(text):
   """Removes HTML or XML character references 
      and entities from a text string.
      keep &amp;, &gt;, &lt; in the source code.
   from Fredrik Lundh
   http://effbot.org/zone/re-sub.htm#unescape-html
   """
   def fixup(m):
      text = m.group(0)
      if text[:2] == "&#":
         # character reference
         try:
            if text[:3] == "&#x":
               return unichr(int(text[3:-1], 16))
            else:
               return unichr(int(text[2:-1]))
         except ValueError:
            pass
      else:
         # named entity
         try:
            text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
         except KeyError:
            pass
      return text # leave as is
   return re.sub("&#?\w+;", fixup, text)

class TextExtractor(HTMLParser.HTMLParser):
    """
    Attempts to extract the main body of text from an HTML document.
    
    This is a messy task, and certain assumptions about the story text must be made:
    
    The story text:
    1. Is the largest block of text in the document.
    2. Sections all exist at the same relative depth.
    """
    
    dom = []
    path = [0]
    pathBlur = 5
    
    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self._ignore = False
        self._ignorePath = None
        self._lasttag = None
        self._depth = 0
        self.depthText = {} # path:text
        self.counting = 0
        self.lastN = 0
        
    def handle_starttag(self, tag, attrs): 
        ignore0 = self._ignore
        tag = tag.lower()
        if tag in ('script','style','option','ul','li','legend','object','noscript','label'): # 'h1','h2','h3','h4','h5','h6',
            self._ignore = True
        attrd = dict(attrs)
        self._lasttag = tag.lower()
        self._depth += 1
        self.path += [self.lastN]
        self.lastN = 0
        
        # Ignore footer garbage.
        if 'id' in attrd and 'footer' in attrd['id'].lower():
            self._ignore = True
        elif 'id' in attrd and 'copyright' in attrd['id'].lower():
            self._ignore = True
        elif 'class' in attrd and 'footer' in attrd['class'].lower():
            self.counting = max(self.counting,1)
            self._ignore = True
        elif 'class' in attrd and 'copyright' in attrd['class'].lower():
            self._ignore = True
            
        # If we just started ignoring, then remember the initial path
        # so we can later know when to start un-ignoring again.
        if self._ignore and not ignore0:
            self._ignorePath = tuple(self.path)
            
    def handle_startendtag(self, tag, attrs):
        pass
    
    def handle_endtag(self, tag):
        if self._ignore and tuple(self.path) == self._ignorePath:
            self._ignore = False
            
        self._depth -= 1
        self.lastN = self.path.pop()
        self.lastN += 1
        
    def handle_data(self, data, entity=False):
        if len(data) > 0 and not self._ignore:
            
            # Skip blocks of text beginning with 'copyright', which usually
            # indicates a copyright notice.
            if data.strip().lower().startswith('copyright') and not self._ignore:
                self._ignore = True
                self._ignorePath = tuple(self.path)
                return
            
            if data:
                
                rpath = tuple(self.path[:-self.pathBlur])
                self.depthText.setdefault(rpath, [])
                self.depthText[rpath] += [data]
                
                # Allow one more layer below, to include
                # text inside <i></i> or <b></b> tags.
                # Unfortuantely, this will include a lot of crap
                # in the page's header and footer, so we'll
                # prefix this text with '#' and strip these out later.
                rpath2 = tuple(self.path[:-self.pathBlur-1])
                self.depthText.setdefault(rpath2, [])
                self.depthText[rpath2] += ['#'+data]
                
    def handle_charref(self, name):
        if name.isdigit():
            text = unescapeHTMLEntities('&#'+name+';')
        else:
            text = unescapeHTMLEntities('&'+name+';')
        self.handle_data(text, entity=True)
                
    def handle_entityref(self, name):
        self.handle_charref(name)
        
    def get_plaintext(self):
        maxLen,maxPath,maxText,maxTextList = 0,None,'',[]
        for path,textList in self.depthText.iteritems():
            
            # Strip off header segments, prefixed with a '#'.
            start = True
            text = []
            for t in textList:
                if len(t.strip()):
                    if t.startswith('#') and start:
                        continue
                    start = False
                text.append(t)
                
            # Strip off footer segments, prefixed with a '#'.
            start = True
            textList = reversed(text)
            text = []
            for t in textList:
                if len(t.strip()):
                    if t.startswith('#') and start:
                        continue
                    start = False
                text.append(t)
            text = reversed(text)
                
            text = ''.join(text).replace('#','')
            try:
                text = text.replace(u'\xa0',' ')
            except UnicodeDecodeError:
                pass
            text = text.replace(u'\u2019',"'")
            text = re.sub("[\\n\\s]+", " ", text).strip() # Compress whitespace.
            #text = re.sub("[\W]+", " ", text).strip() # Compress whitespace.
            maxLen,maxPath,maxText,maxTextList = max((maxLen,maxPath,maxText,maxTextList), (len(text),path,text,textList))
        
        return maxText
    
    def error(self,msg):
        # ignore all errors
        pass

class HTMLParserNoFootNote(htmllib.HTMLParser):
    """
    Ignores link footnotes, image tags, and other useless things.
    """
    
    textPattern = None
    path = [0]
    
    def handle_starttag(self, tag, attrs, *args):
        time.sleep(0.5)
        self.path += [0]
        if tag == 'script':
            pass

    def handle_endtag(self, tag, *args):
        self.path.pop()
        self.path[-1] += 1
        if tag == 'script':
            pass
    
    def anchor_end(self):
        if self.anchor:
            #self.handle_data("[%d]" % len(self.anchorlist))
            self.anchor = None
            
    def handle_image(self, src, alt, *args):
        pass
    
    def handle_data(self, data):
        if self.textPattern:
            data = ' '.join(self.textPattern.findall(data))
        htmllib.HTMLParser.handle_data(self, data)
    
def extractFromHTML(html):
    """
    Extracts text from HTML content.
    """
    
    # create memory file
    file = StringIO.StringIO()
    
    # convert html to text
    f = formatter.AbstractFormatter(formatter.DumbWriter(file))
    p = TextExtractor()
    try:
        p.feed(html)
    except (AttributeError,  AssertionError):
        return None
    
    p.close()
    text = p.get_plaintext()
    
    text = re.sub("\s[\(\),;\.\?\!](?=\s)", " ", text).strip() # Remove stand-alone punctuation.
    text = re.sub("[\n\s]+", " ", text).strip() # Compress whitespace.
    text = re.sub("\-{2,}", "", text).strip() # Remove consequetive dashes.
    text = re.sub("\.{2,}", "", text).strip() # Remove consequetive periods.
    return text

def tidyHTML(dirtyHTML):
    """
    Runs an arbitrary HTML string through Tidy.
    """
    file = StringIO.StringIO()
    options = dict(output_xhtml=1, add_xml_decl=1, indent=1, tidy_mark=1)
    html = tidy.parseString(dirtyHTML, **options)
    html.write(file)
    html = file.getvalue()
    return html

def extractFromURL(url):
    """
    Extracts text from a URL.
    """
    try:
        stream = urllib2.urlopen(url)
        html = stream.read()
        stream.close()
    except (ValueError, IOError, httplib.InvalidURL, httplib.BadStatusLine,
            socket.timeout, socket.error):
        return None
    
    # Convert content to XHTML.
    html = tidyHTML(html)
    extracted_content = extractFromHTML(html)
    try:
        return str(extracted_content)
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None
