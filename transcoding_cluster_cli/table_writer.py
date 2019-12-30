# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import shutil
import os
import subprocess
import sys
import re
import enum

class AnsiColor( enum.Enum ):
    default = 39
    black = 30
    red = 31
    green = 32
    yellow = 33
    blue = 34
    magenta = 35
    cyan = 36
    white = 37
    bright_black = 90
    bright_red = 91
    bright_green = 92
    bright_yellow = 93
    bright_blue = 94
    bright_magenta = 95
    bright_cyan = 96
    bright_white = 97

class TableCell( object ):
    def __init__(self, data="", row=None, column=None ):
        self.data = data
        self.row = row
        self.column = column

        self.color = None # member of AnsiColor
        self.textWrap = None # if True, avoid wrapping
        self.isHeader = None # same as <th></th> vs <td></td>
        self.width = None # fixed width, None = auto-width
    
    def getCellAttr(self, name, fallback=None ):
        for item in [ self, self.row, self.column ]:
            value = getattr( item, name, None )
            if value is not None:
                return value
        return fallback

class TableRow( object ):
    def __init__(self, color=None, wrap=None, header=None ):
        self.cells = []

        self.color = color # member of AnsiColor
        self.textWrap = wrap # if True, avoid wrapping
        self.isHeader = header # same as <th></th> vs <td></td>

class TableColumn( object ):
    def __init__(self, table, index, color=None, wrap=None, header=None, width=None ):
        self.table = table
        self.index = index
        
        self.color = color
        self.textWrap = wrap # if True, avoid wrapping
        self.isHeader = header # same as <th></th> vs <td></td>
        self.width = None # fixed width, None = auto-width

class TableCellIter( object ):
    def __init__(self, table, row=None, col=None, count=None ):
        self.table = table
        self.axes = [ self.table.rows, self.table.columns ]
        self.count = count
        self.idx = [ 0, 0 ]
        self.axis = 0
        self.scan = False
        
        if row is None and col is None:
            self.scan = True
        elif row is not None and col is not None:
            self.idx = [ row, col ]
            self.count = 1
        elif row is not None:
            self.idx = [ row, 0 ]
            self.axis = 1
        elif col is not None:
            self.idx = [ 0, col ]
            self.axis = 0

    @property
    def row(self):
        return self.idx[ 0 ]
    @property
    def column(self):
        return self.idx[ 1 ]
    
    def __iter__(self):
        return self
    def __next__(self):
        if self.count is not None: 
            if self.count <= 0:
                raise StopIteration
            self.count -= 1

        otherAxis = (self.axis+1 )%2
        if self.idx[ self.axis ] >= len( self.axes[ self.axis ] ):
            if not self.scan:
                raise StopIteration
            self.idx[ self.axis ] = 0
            self.idx[ otherAxis ] += 1
        if self.idx[ otherAxis ] >= len( self.axes[ otherAxis ] ):
            raise StopIteration

        cell = self.table.rows[ self.idx[0] ].cells[ self.idx[1] ]
        self.idx[ self.axis ] += 1
        return cell

class Table( object ):
    def __init__(self):
        self.rows = []
        self.columns = []

    @property
    def width(self):
        return len( self.columns )
    @property
    def height(self):
        return len( self.rows )
    
    def iterCells(self, **kwargs ):
        return TableCellIter( self, **kwargs )
    
    def appendRow(self, data, **kwargs ):
        # autogenerate columns
        delta = len( data ) - self.width
        if delta > 0:
            self.columns.extend( [ TableColumn(self, self.width+i ) for i in range(delta) ] )

        # put data into cells and append 
        row = TableRow( **kwargs )
        for i in range( len(data) ):
            if isinstance( data[i], TableCell ):
                cell = data[ i ]
                cell.row = row
                cell.column = self.columns[ i ]
            else:
                cell = TableCell( data=data[i], row=row, column=self.columns[i] )
            row.cells.append( cell )

        self.rows.append( row )
        return row

class AnsiToken( object ):
    def __init__(self, name, start, value):
        self.name = name
        self.start = start
        self.value = value
    
    @property
    def len(self):
        return len( self.value )

    @property
    def renderLen(self):
        if self.name in [ 'eof', 'ansi' ] or self.value == '\n':
            return 0
        return len( self.value )

class AnsiOffset( object ):
    def __init__(self, off=0, renderOff=0 ):
        self.off = off
        self.render = renderOff
    
    def __iadd__(self, other ):
        self.off += other.off
        self.render += other.render
        return self
    
class AnsiLexer( object ):
    def __init__(self, text ):
        self.text = text
        self.ansiRe = re.compile( r'((?:\x9B|\x1B\[)[0-?]*[ -/]*[@-~])' )
        self.nextAnsi = self.ansiRe.search( self.text )
        self.idx = 0
        self.tok = AnsiToken( 'null', 0, '' )
    
    def __iter__(self):
        return self
    def __next__(self):
        if self.tok.name == 'eof':
            raise StopIteration
        if self.nextAnsi is not None and self.nextAnsi.start() == self.idx:
            self.tok = AnsiToken( 'ansi',  self.nextAnsi.start(), self.nextAnsi.group(0) )
        elif self.idx >= len( self.text ):
            self.tok = AnsiToken( 'eof', len( self.text ), '' )
        else:
            self.tok = AnsiToken( 'char', self.idx, self.text[ self.idx ] )

        if self.nextAnsi is not None and self.nextAnsi.start() <= self.idx:
            self.nextAnsi = self.ansiRe.search( self.text, self.idx )
        self.idx += self.tok.len
        return self.tok

class TableDataFormatter( object ):
    def __init__(self):
        # https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python
        self.ansiRe = re.compile(r'((?:\x9B|\x1B\[)[0-?]*[ -/]*[@-~])')
    
    def colorFormat(self, color):
        if isinstance(color, AnsiColor ):
            return "\033[{}m".format( color.value )
        raise ValueError( "Color not supported" )
    
    def renderedTextLen(self, text ):
        return len( self.ansiRe.sub('', text.replace( "\n", "" ) ) )
    
    def splitLines(self, text ):
        return text.split( "\n" )
    
    def saveTextFormat(self, text, previous="" ):
        fmt = previous
        for m in self.ansiRe.finditer( text ):
            fmt = m.group( 0 )
        return fmt
    
    def decolorText(self, text ):
        return self.ansiRe.sub( '', text )
    
    def wrapText(self, text, start, width ):
        if start > len( text ):
            raise IndexError( "start index must be lesser than the text length" )
        offset = AnsiOffset()
        text = text[ start: ]

        # wrap at word boundary using the greedy algorithm
        word = AnsiOffset()
        for tok in AnsiLexer( text ):
            word += AnsiOffset( tok.len, tok.renderLen )
            
            if tok.name == 'eof' or tok.value == ' ':
                if offset.render + word.render <= width:
                    # word fits on the line: commit word 
                    offset += word
                    word = AnsiOffset()
                else:
                    break
            elif tok.value == '\n':
                # transform \n into hard line break
                offset += word
                break

        if offset.off == 0:
            # first word is too big to fit into a line: force wrap 
            for tok in AnsiLexer( text ):
                offset += AnsiOffset( tok.len, tok.renderLen )
                if offset.render >= width:
                    break
        
        return start + offset.off

class TableWriter( object ):
    def __init__(self):
        self.table = Table()
        self.formatter = TableDataFormatter()
        self.width = shutil.get_terminal_size( fallback=(80,24) ).columns
        self._colorEnabled = None
        
        self.borders = { 
            "hor": "─", "ver": " │ ", "int": "┼",
            "top": "─", "right": " │", "down": "─", "left": "│ ",
            "top_int": "┬", "right_int": "─┤", "down_int": "┴", "left_int": "├─",
            "top_left": "┌─", "top_right": "─┐", "down_right": "─┘", "down_left": "└─"
        }
        self.fillerChar = " " # "•"
        self.headerContentFormat = "\033[1m" # bold text
        self.borderFormat = "\033[0m" # reset all
        
    @property
    def colorEnabled(self):
        if self._colorEnabled is None:
            self.colorEnabled = True
        return self._colorEnabled
    
    @colorEnabled.setter
    def colorEnabled(self, value ):
        self._colorEnabled = False
        if os.isatty( sys.stdout.fileno() ):
            p = subprocess.run( [ "tput", "colors" ], stdout=subprocess.PIPE )
            self._colorEnabled = p.returncode == 0 and int( p.stdout.decode() ) > 0
            self._colorEnabled = self._colorEnabled and value 
    
    def setConf(self, i, j, name, value):
        """legacy interface, i:row, j:col, i=None:allRows, j=None:allCols"""
        if name == "color":
            value = AnsiColor[ value ]
        aMap = { "color": "color", "wrap": "textWrap", "heading": "isHeader", "width": "width" }

        if i == None and j == None:
            raise TypeError( "i and j cannot be None at the same time" )
        if i == None:
            setattr( self.table.columns[ j ], aMap[ name ], value )
        elif j == None:
            setattr( self.table.rows[ i ], aMap[ name ], value )
        else:
            setattr( self.table.rows[ i ].cells[ j ], aMap[ name ], value )

    def appendRow(self, row ):
        """ legacy interface """ 
        return self.table.appendRow( row )
    
    def horizontalBorderLine(self, colWidths, rowIndex ):
        wbHalf = lambda name: max(1, int( (len(self.borders[name])-1) / 2 ) )
        wbFull = lambda name: len( self.borders[name] )
        (bl, br, bn, bi) = ( "left_int", "right_int", "hor", "int" )
        if rowIndex == 0:
            (bl, br, bn, bi) = ( "top_left", "top_right", "top", "top_int" )
        elif rowIndex == self.table.height:
            (bl, br, bn, bi) = ( "down_left", "down_right", "down", "down_int" )

        line = self.borderFormat + self.borders[ bl ]
        for w in range( len( colWidths ) ):
            be = bi
            if w == 0:
                n = colWidths[ w ] + wbFull( "left_int" ) - wbHalf( "int" )
            elif w == len(colWidths)-1:
                n = colWidths[ w ] + wbFull( "right_int"  ) - wbHalf( "int" )
                be = br
            else:
                n = colWidths[ w ] + wbFull( "ver" ) - wbFull( "int" )
            line += ( self.borders[ bn ] * n ) + self.borders[ be ]
        return line
    
    def verticalBorder(self, colIndex ):
        b = { colIndex: "ver" }
        b.update( { 0: "left", self.table.width: "right" } )
        return self.borders[ b[ colIndex ] ]
    
    class AverageList( object ):
        def __init__(self, size):
            self.data = [ [0,0] for i in range(size) ]

        def putValues(self, index, values ):
            self.data[ index ][0] += sum( values )
            self.data[ index ][1] += len( values )

        def __getitem__(self, index ):
            return self.data[ index ][0] * 1.0 / self.data[ index ][1]

        def sum(self):
            return sum( map( lambda d: d[0]*1.0/d[1] , self.data ) )
        
        def filter(self, func ):
            a = self.__class__( 0 )
            indices = filter( func, range( len( self.data ) ) )
            a.data = map( lambda i: self.data[i], indices )
            return a
    
    def calcColumnWidths(self):
        form = self.formatter
        # avg/max line width of lines withing a cell as if no wrapping done
        avgColWidth = self.AverageList( self.table.width )
        maxColWidth = [ 0 for i in self.table.columns ]
        # use flexible col-width & wrapping or use max col-width to avoid wrapping
        doColTextWrap = [ False for i in self.table.columns ]
        # final calculated column widths
        widths = [ 0 for i in self.table.columns ]
        # minimum total width required for the cell data only
        totalMinWidth = 0

        iter = self.table.iterCells()
        for cell in iter:
            if cell.getCellAttr( "width" ) is not None:
                lines = [ cell.getCellAttr( "width" ) ]
            else:
                lines = [ form.renderedTextLen( l ) for l in form.splitLines( cell.data ) ]

            avgColWidth.putValues( iter.column, lines )
            maxColWidth[ iter.column ] = max( maxColWidth[ iter.column ], max( lines ) )
            if not doColTextWrap[ iter.column ] and cell.getCellAttr( "textWrap", fallback=False ):
                doColTextWrap[ iter.column ] = True

        for c in range( self.table.width ):
            if not doColTextWrap[ c ]:
                # use widest cell if wrapping is to be avoided
                widths[ c ] = maxColWidth[ c ]
            elif self.table.columns[ c ].width is not None:
                # set width of fixed width columns
                widths[ c ] = self.table.columns[ c ].width
            else:
                # flex width: 10% of avg as min-width heuristic to avoid absurd long cells
                widths[ c ] = int( avgColWidth[ c ]*0.1 )
            totalMinWidth += widths[ c ]
        
        freeWidth = self.width - totalMinWidth 
        freeWidth -= ( self.table.width - 1 ) * len( self.borders[ "ver" ] )
        freeWidth -= ( len( self.borders["left"] ) + len( self.borders["right"] ) )
        if freeWidth < 0:
            raise RuntimeError( "Terminal is not wide enough to view this table" )
        
        avgWWSum = avgColWidth.filter( lambda i: doColTextWrap[i] ).sum()
        # distribute remaining width to flex columns, use the average cell size as ratio
        for c in range( self.table.width ):
            if doColTextWrap[ c ]:
                widths[ c ] += min( maxColWidth[c], int( (avgColWidth[c]/avgWWSum)*freeWidth ) )
        
        return widths
    
    def display(self):
        """ legacy interface """ 
        form = self.formatter
        colWidths = self.calcColumnWidths()
        
        print( self.horizontalBorderLine( colWidths, 0 ) )
        for row in range( self.table.height ):
            cellIdx = [ 0 for i in range( self.table.width ) ]
            cellAnsi = [ "" for i in range( self.table.width ) ]
            hasCellData = True

            while hasCellData:
                line = self.borderFormat + self.verticalBorder( 0 )

                hasCellData = False
                for cell in self.table.rows[ row ].cells:
                    c = cell.column.index

                    # set cell color and restore cell data color from previous line
                    color = cell.getCellAttr( "color", fallback=AnsiColor.default )
                    line += form.colorFormat( color ) + cellAnsi[ c ]
                    # format cell as header cell (bold)
                    if cell.getCellAttr( "isHeader", fallback=False ):
                        line += self.headerContentFormat
                    # fetch the cell data for this line
                    startIdx = cellIdx[c]
                    cellIdx[c] = form.wrapText( cell.data, startIdx, colWidths[c] )
                    cellLine = cell.data[ startIdx : cellIdx[c] ].replace( "\n", "" )
                    # save last ansi escape sequence for next line
                    cellAnsi[ c ] = form.saveTextFormat( cellLine, previous=cellAnsi[c] )
                    # pad line to column width (considering ansi escape sequences)
                    cellLineWidth = form.renderedTextLen( cellLine ) 
                    line += cellLine + ( colWidths[c] - cellLineWidth )*self.fillerChar
                    # reset color and print right vertical border
                    line += self.borderFormat + self.verticalBorder( c+1 )
                    # one more line needed
                    if cellIdx[c] < len( cell.data ):
                        hasCellData = True
                
                # remove all color if color is disabled/unavailable
                if not self.colorEnabled:
                    line = form.decolorText( line )
                print( line )
            print( self.horizontalBorderLine( colWidths, row+1 ) )
