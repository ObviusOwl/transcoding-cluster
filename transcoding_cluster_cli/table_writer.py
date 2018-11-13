# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import shutil
import os
import subprocess
import sys
import math
import re

class TableWriter(object):
    def __init__(self):
        self.width = self.getTerminalSize().columns
        self.rows = []
        self.data = []
        self.colConf = []
        self.rowConf = []
        self.colNum = 0
        self.border_vert = " │ "
        self.border_hor = "─"#" "#
        self.border_inter = "┼"#"│"#
        self.empty_char = " " #"•"
        self.hasHeader = False
        self.colors = {
            "default" : "\033[39m",
            "red"     : "\033[31m",
            "green"   : "\033[32m",
            "yellow"  : "\033[33m",
            "blue"    : "\033[34m"
        }
        # https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
        self.ansiRe = re.compile(r'((?:\x9B|\x1B\[)[0-?]*[ -/]*[@-~])')
        self.headerContentFormat = "\033[1m" # bold text
        self.borderFormat = "\033[0m" # reset all
        self.hasColor = self.guessColorEnabled()
    
    def getTerminalSize(self, fallback=(80,24) ):
        # first try python3 native
        get_terminal_size = getattr(shutil, "get_terminal_size", None)
        if callable( get_terminal_size ):
            return get_terminal_size( fallback=fallback )

        # fallback for python 2
        # https://stackoverflow.com/questions/566746/how-to-get-linux-console-window-width-in-python
        import fcntl, termios, struct
        from collections import namedtuple
        terminal_size = namedtuple("terminal_size", ["columns","lines"])
        try:
            h, w, hp, wp = struct.unpack('HHHH'.encode('ascii'), fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack('HHHH'.encode('ascii'), 0, 0, 0, 0)))
            return terminal_size(columns=w, lines=h)
        except IOError as e:
            return terminal_size(columns=fallback[0], lines=fallback[1])


    def stripAnsi(self, line):
        # https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
        #ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]')
        return self.ansiRe.sub('', line)
    
    def guessColorEnabled(self):
        r1 = os.isatty( sys.stdout.fileno() )
        if r1 == False:
            return False
        p = subprocess.Popen(["tput","colors"], stdout=subprocess.PIPE)
        r2 = int(p.communicate()[0].decode())
        return ( r1 and p.returncode == 0 and r2 > 0 )
        
    def setColorOutput(self, value ):
        r = self.guessColorEnabled()
        self.hasColor = (value and r)
    
    def getConf(self, i, j, name):
        """ i: row, j:col, name:configNameString"""
        e = self.data[i][j]
        if name in e and e[name] != None:
            return e[name]
        elif name in self.rowConf[i] and self.rowConf[i] != None:
            return self.rowConf[i][ name ]
        elif name in self.colConf[j] and self.colConf[j] != None:
            return self.colConf[j][ name ]
        elif name in e:
            return e[name] # None
        else:
            raise TypeError( "unknown config id string" )
    
    def setConf(self, i, j, name, value):
        """ i:rowIdx, j:colIdx, name:configID, i=None:allRows, j=None:allCols"""
        if i == None and j == None:
            raise TypeError( "i and j cannot be None at the same time" )
        if i == None:
            self.colConf[j][ name ] = value
        elif j == None:
            self.rowConf[i][ name ] = value
        else:
            self.data[i][j][ name ] = value
        
    def getDefaultConf(self, data):
        return {"data":data, "color":None, "wrap":None,"heading":None} # colspan?
    def getDefaultColConf(self):
        return {"color":None, "wrap":None,"heading":None}
    def getDefaultRowConf(self):
        return {"color":None, "wrap":True,"heading":None}
    
    def appendRow(self, row ):
        r = []
        # copy element data
        for e in row:
            #r.append( self.getDefaultConf(e.encode("UTF-8")) )
            r.append( self.getDefaultConf(e) )
        # grow colConf to new col count
        if len(r) > len(self.colConf):
            for i in range( len(r)-self.colNum ):
                self.colConf.append( self.getDefaultColConf() )
        # append to data and add new config defaults
        self.data.append( r )
        self.rowConf.append( self.getDefaultRowConf() )
            
    def wrapText( self, text, startIndex ,maxWidth ):
        assert startIndex < len(text)
        offset = 0
        # hard-wrap: transofrm newlines into wraps
        lfIdx = text[startIndex:].find( "\n" )
        if lfIdx >= 0 and lfIdx+1 <= maxWidth:
            return startIndex + lfIdx+1
        # split words for soft-wrap
        words = text[startIndex:].split(" ")
        # wrap words with greedy algorithm
        for i in range(len(words)):
            wordDisp = self.stripAnsi( words[i] )
            spaceWidth = 1
            if i == len(words)-1:
                spaceWidth = 0
            if offset+len(wordDisp)+spaceWidth <= maxWidth:
                offset += (len(words[i])+spaceWidth)
            else:
                break
        # first word is too big to fit into maxWidth -> force wrap
        if offset == 0 and len(words) > 0:
            word = words[0]
            m = self.ansiRe.search( word )
            if m == None:
                offset = maxWidth
            elif m != None and m.start() > maxWidth:
                offset = maxWidth
            else:
                offset = m.end()
                while True:
                    m = self.ansiRe.search( word, offset )
                    if m == None:
                        break
                    wordDisp = self.stripAnsi( word[ : m.end() ] )
                    if len( wordDisp ) > maxWidth:
                        break
                    offset = m.end()
        return startIndex + offset
    
    def print_borderline(self, colMaxWidth ):
        border = ""
        if self.hasColor:
            border += self.borderFormat + self.colors["default"]
        border += int(math.floor(len(self.border_vert)/2))*" "+self.border_inter
        for i in range( len(self.colConf) ):
            border += self.border_hor*(colMaxWidth[i]+len(self.border_vert)-1)
            border += self.border_inter
        print( border )
        
    
    def display(self):
        avgs = []
        maxs = []
        # init 
        for i in range( len(self.colConf) ):
            avgs.append( 0 )
            maxs.append( 0 )
        # sum element length for all cols
        for row in self.data:
            for i in range(len(row)):
                # get content length honoring hard-wraps
                contLen = 0
                for line in row[i]["data"].split("\n"):
                    if len(line) > contLen:
                        contLen = len(line)
                avgs[i] += contLen
                if maxs[i] < contLen:
                    maxs[i] = contLen
        # get average content length per col
        s =0
        for i in range( len(self.colConf) ):
            avgs[i] = avgs[i] / len( self.data )
            if self.colConf[i]["wrap"] == False:
                avgs[i] = maxs[i]
            else:
                s += avgs[i] # do not add no-wrap cells

        # get the remaining space to distribute to wrapping cols
        flexWidth = self.width-len(self.border_vert)
        for i in range( len(self.colConf) ):
            if self.colConf[i]["wrap"] == False:
                flexWidth -= maxs[i]
                flexWidth -= len(self.border_vert)
        if (flexWidth - self.colNum*len(self.border_vert)) <= 0 :
            raise RuntimeError( "Terminal is not wide enough to view this table" )        

        # finally distribute the available term width to all cols
        colMaxWidth = []
        for i in range( len(self.colConf) ):
            if self.colConf[i]["wrap"] == False:
                colMaxWidth.append( maxs[i] ) 
            else:
                per = (avgs[i]/(s*1.0))*flexWidth
                colMaxWidth.append( int(math.floor(per)) - len(self.border_vert) )
                # if average*width < 1char => column cannot be negative width
                if colMaxWidth[-1] <= 0:
                    colMaxWidth[-1] = 1
                    s += 1
        
        self.print_borderline(colMaxWidth)
        rowCount = 0
        for row in self.data:
            # init list of indices up to where cell content has been printed
            rowIdx = []
            rowAnsi = []
            for i in range( len(self.colConf) ):
                rowIdx.append(0)
                rowAnsi.append(None)
            # print lines while cells have data
            data = True
            while data == True:
                line = ""
                # add leftmost border
                if self.hasColor:
                    line = self.colors["default"] 
                line += self.border_vert
                t = True
                # print cell content for this line
                for i in range( len(row) ):
                    # set the right color
                    color = self.getConf( rowCount, i, "color" )
                    head  = self.getConf( rowCount, i, "heading" )
                    # set cell content color
                    if self.hasColor and color != None and color in self.colors:
                        line += self.colors[ color ]
                    # set bold for heading cells
                    if self.hasColor and head == True:
                        line += self.headerContentFormat 
                    if rowIdx[i] < len( row[i]["data"] ):
                        # if there is data in the cell row[i]
                        b = rowIdx[i] # start index of new content
                        rowIdx[i] = self.wrapText( row[i]["data"], rowIdx[i], colMaxWidth[i] )
                        e = rowIdx[i] # end index of new content
                        assert not ( (b == e) and (e != len(row[i]["data"])) )
                        # ansi colors use up space but are not displayed -> fill up with extra empty chars
                        nullDelta = len(row[i]["data"][b:e]) - len(self.stripAnsi( row[i]["data"][b:e] ))
                        # restore ansi color from previous line
                        if rowAnsi[i] != None:
                            line += rowAnsi[i]
                        line += row[i]["data"][b:e].replace("\n","").ljust( colMaxWidth[i]+nullDelta, chr(ord(self.empty_char)) )
                        # save ansi color for next line
                        rowAnsi[i] = None
                        m = self.ansiRe.findall( row[i]["data"][b:e] )
                        if m != None and len(m) > 0:
                            rowAnsi[i] = m[-1]
                    else:
                        line += (self.empty_char*colMaxWidth[i])
                    # add cell border 
                    if self.hasColor:
                        line += self.borderFormat + self.colors["default"] 
                    line += self.border_vert
                    if rowIdx[i] < len( row[i]["data"] ):
                        t = False # there is still content to distribute in this cell
                if t == True:
                    # t has not been touched by any cell of this row, 
                    # so all cells have been completely printed
                    data = False
                print(line)
            self.print_borderline(colMaxWidth)
            rowCount += 1
