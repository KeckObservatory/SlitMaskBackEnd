# because without re life itself would be impossible
import re

# we use the filesystem
import os

import datetime

# debugging
import time

# this file is only partly translated
# it began as procedural Tcl
# it is trying to become object python

class Gnuplot5():
    """
    an object whose methods use gnuplot to make a SVG plot of a mask
    """

    ########################################################################

    def __init__(self):

        # gnuplot file pointer
        self.gpfp       = None

        # gnuplot file name
        self.gpfn       = None

        ########################################################################

        # we need places where this web-server routine can write temporary files

        # TmpPlotCmdDir is for the plot command files
        # the web server process must be able to write here
        # the plot program must be able to read here
        # these files can be deleted any time after the plot program executes them
        # for the sake of debugging, delete might be a cron job that waits an hour or more
        self.TmpPlotCmdDir  = "/tmp"

        # TmpPlotWebDir is for the plot output files
        # the plot program must be able to write here
        # the web server must be able to serve things here embedded in a web page
        # these files need to persist until a web user is done looking at them
        # delete might be a cron job that waits a day or more
        self.TmpPlotWebDir  = "/tmp"

    # end def __init__()

    ########################################################################

    def __del__(self):

        if (self.gpfp):
            self.CloseSVG()
        # end if

    # end def __del__()

    ########################################################################

    def OpenSVG(self, froot):

        # we need a temporary file name for the command and plot files
        # for debugging we want this to be recognizable to a human
        # we do not expect as many as one file per second

        pid     = os.getpid()

        # file name is long, but human readable for debugging
        # please clear TmpPlotCmdDir occasionally
        isodt   = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%dT%H%M%S%f')

        self.froot      = froot

        self.fname      = "%s%sp%s" % (froot, isodt, pid)

        # assume POSIX, not bothering with os.path.join
        self.gpfn       = "%s/%s.gnup" % (self.TmpPlotCmdDir, self.fname)

        #print("opening")

        # this needs to be in try/except after development
        self.gpfp       = open(self.gpfn, 'w')

        #print("opened fn %s fp %s" % (self.gpfn, self.gpfp))

    # end def OpenSVG()

    ########################################################################


    def CloseSVG(self):

        # plot a line way below the viewport
        # this tells gnuplot to render the mask and all those slitlets
        self.gpfp.write("plot -1000 notitle\n")

        self.gpfp.flush()
        #print("close flushed")

        #print("dir")
        #print(dir(self.gpfp))

        #print("pprint")
        #import pprint
        #pprint.pprint(vars(self.gpfp))

        #print("close sleeping")
        #time.sleep(20)
        #print("closing")

        self.gpfp.close()

        #print("closed")

        self.gpfp       = None

        return self.gpfn

    # end def CloseSVG()

    ########################################################################

    def Header(self, instrume, bluid, bluname, guiname):
        """
        write the header boilerplate of a gnuplot 5.4 file
        """

        # the file name is based on the e-mail address, but
        # gnuplot only accepts alphanumeric and _ in a plot name
        sroot       = re.sub("[^A-Za-z0-9_]", "_", self.froot)

        # x1,x2,y1,y2: world coordinate limits for mask plots
        # these use the coordinate system on the metal of the masks
        # units are [mm]
        # svgx,svgy: the default pixel sizes of the SVG plots
        # these do not exactly match the aspect ratio of the
        # actual metal because we are not trying to constrain
        # nor calculate the size of the gnuplot margins
        if instrume == "LRIS":
             x1     = -10
             x2     = 365
             y1     = -10
             y2     = 275
             svgx   = 720
             svgy   = 600
        else:
            # we are DEIMOS
            x1      = -385.0
            x2      =  410.0
            y1      =  -10
            y2      =  240.0
            svgx    =  980
            svgy    =  360
        # end if

        # gnuplot 5.4
        # including the javascript wastes some disk space but makes the plots fully standalone
        self.gpfp.write("set terminal svg size %.3f,%.3f dynamic enhanced font 'arial,10' mousing standalone name '%s' butt dashlength 1.0\n" % (svgx, svgy, sroot))

        self.gpfp.write("set output '%s/%s.svg'\n" % (self.TmpPlotWebDir, self.fname))
        self.gpfp.write("set title 'Plot of SlitMask blueprint %s %s (%s)' noenhanced\n" % (bluid, bluname, guiname))
        self.gpfp.write("set xrange [ %.3f : %.3f ]\n" % (x1, x2))
        self.gpfp.write("set yrange [ %.3f : %.3f ]\n" % (y1, y2))

        self.gpfp.write("set xlabel 'mill X [mm]'\n")
        self.gpfp.write("set mxtics\n")
        self.gpfp.write("set ylabel 'mill Y [mm]'\n")
        self.gpfp.write("set mytics\n")
        self.gpfp.write("\n")
        # draw background objects and their hypertext before slitlets and holes
        self.gpfp.write("bgnum = 0\n")
        # draw slitlets and holes after background objects
        self.gpfp.write("fgnum = 100000\n")
        self.gpfp.write("\n")
        self.gpfp.write("set style line 2 lt rgbcolor 'black'\n")
        self.gpfp.write("set style line 3 lt rgbcolor 'skyblue'\n")
        self.gpfp.write("set style line 4 lt rgbcolor 'red'\n")

        # set linetype 7 for use with label hypertext
        self.gpfp.write("set linetype 7 lc bgnd lw 1 ps 1 pt 7\n")

        # when the calling program wants to embed the svg into html
        # the calling program needs to know the default pixel size
        return svgx,svgy

    # end def Header()

    ########################################################################

    def DrawSlit(self, hue, x1, y1, x2, y2, x3, y3, x4, y4, dslitid):
        # slitlets are quadrilaterals
        # dslitid is for later use with SVG hypertext hover
        # this whole notion of SVG via gnuplot was just playing around
        # except that when the Sparc/Solaris webserver died the original
        # ploticus code died with it, so only the gnuplot SVG remained.

        self.gpfp.write("bgnum = bgnum + 1\n")

        # hypertext dslitid
        # lt 7 is set to background color in header
        # pt 7 is a filled circle which will trigger hover hypertext
        xc      = (x1 + x3)*0.5
        yc      = (y1 + y3)*0.5
        self.gpfp.write("set label bgnum at %.3f,%.3f '%s' point lt 7 pt 7 ps 1 hypertext\n" % (xc,yc,dslitid))

        # increment the object number so that the slitlet is drawn after
        # the point that acts as its background for the hypertext
        self.gpfp.write("fgnum = fgnum + 1\n")

        # draw the slitlet
        self.gpfp.write("set object fgnum polygon from %.3f,%.3f"    % (x1,y1))
        self.gpfp.write(" to %.3f,%.3f"                               % (x2,y2))
        self.gpfp.write(" to %.3f,%.3f"                               % (x3,y3))
        self.gpfp.write(" to %.3f,%.3f"                               % (x4,y4))
        self.gpfp.write(" to %.3f,%.3f\n"                             % (x1,y1))

        self.gpfp.write("set object fgnum fillcolor '%s' fillstyle solid noborder\n" % (hue))

        # blank line to make the gnuplot code readable
        self.gpfp.write("\n")

        #print("slit %s %.3f %.3f %s" % (hue, x1, y1, dslitid))

    # end def DrawSlit()

    ########################################################################

    def DrawHole(self, hue, x1, y1, x2, y2, x3, y3, x4, y4, dslitid):
        # holes are just dots on these  plots
        # dslitid is for later use with SVG hypertext hover
        # this whole notion of SVG via gnuplot was just playing around
        # except that when the Sparc/Solaris webserver died the original
        # ploticus code died with it, so only the gnuplot SVG remained.

        self.gpfp.write("bgnum = bgnum + 1\n")

        # hypertext dslitid
        # lt 7 is set to background color in header
        # pt 7 is a filled circle which will trigger hover hypertext
        xc      = (x1 + x3)*0.5
        yc      = (y1 + y3)*0.5
        self.gpfp.write("set label bgnum at %.3f,%.3f '%s' point lt 7 pt 7 ps 1 hypertext\n" % (xc,yc,dslitid))

        # increment the object number so that the hole is drawn after
        # the point that acts as its background for the hypertext
        self.gpfp.write("fgnum = fgnum + 1\n")

        # 1 mm is way bigger than the tool and hole diameter, but this is just a schematic
        self.gpfp.write("set object fgnum circle center %.3f,%.3f size 1" % (xc,yc))

        self.gpfp.write("set object fgnum fillcolor '%s' fillstyle solid noborder\n" % (hue))

        # blank line to make the gnuplot code readable
        self.gpfp.write("\n")

    # end def DrawSlit()

    ########################################################################

    def DrawMaskOutline(self, instrume):

        if instrume == "LRIS":
            gnuDraw_LRIS_Outline(self)
        else:
            gnuDraw_DEIMOS_Outline(self)
        #end if

    # end def DrawMaskOutline()

    ########################################################################

    def Draw_LRIS_Outline(self):

        # this is the raw metal
        # refer to Caltech drawings labelled
        #       108684 KECK 401
        #       108811 KECK 452

        self.gpfp.write("\n")

        # start at the lower left corner
        # right along the bottom to the lower right corner
        self.gpfp.write("set arrow from    0  ,  0       to  355.6  ,  0     nohead ls 2\n")
        # up along the right to the upper right corner
        self.gpfp.write("set arrow from  355.6,  0       to  355.6  ,264.668 nohead ls 2\n")
        # next back left to upper left corner
        self.gpfp.write("set arrow from  355.6,264.668   to    0    ,264.668 nohead ls 2\n")
        # down along the left to finish back at the lower left corner
        self.gpfp.write("set arrow from    0  ,264.668   to    0    ,  0     nohead ls 2\n")

        self.gpfp.write("\n")

        # this is the useful area
        # indicate the approximate vignetting by the bar in the middle
        self.gpfp.write("set arrow from  174.625,  0.    to  174.625,264.668 nohead ls 3\n")
        self.gpfp.write("set arrow from  180.975,  0.    to  180.975,264.668 nohead ls 3\n")

        # indicate the approximate vignetting by the pickoff mirror
        # screw
        self.gpfp.write("set arrow from  174.625, 62.69  to  172.085, 62.69  nohead ls 3\n")
        self.gpfp.write("set arrow from  172.085, 62.69  to  172.085, 67.77  nohead ls 3\n")
        self.gpfp.write("set arrow from  172.085, 67.77  to  174.625, 67.77  nohead ls 3\n")
        # screw
        self.gpfp.write("set arrow from  174.625,100.79  to  172.085,100.79  nohead ls 3\n")
        self.gpfp.write("set arrow from  172.085,100.79  to  172.085,105.87  nohead ls 3\n")
        self.gpfp.write("set arrow from  172.085,105.87  to  174.625,105.87  nohead ls 3\n")
        # frame
        self.gpfp.write("set arrow from  180.975, 58.88  to  187.325, 58.88  nohead ls 3\n")
        self.gpfp.write("set arrow from  187.325, 58.88  to  187.325, 96.98  nohead ls 3\n")
        self.gpfp.write("set arrow from  187.325, 96.98  to  212.725, 96.98  nohead ls 3\n")
        self.gpfp.write("set arrow from  212.725, 96.98  to  212.725,109.68  nohead ls 3\n")
        self.gpfp.write("set arrow from  212.725,109.68  to  180.975,109.68  nohead ls 3\n")
        # mirror
        self.gpfp.write("set arrow from  212.725, 96.98  to  212.725, 71.58  nohead ls 3\n")
        self.gpfp.write("set arrow from  212.725, 71.58  to  187.325, 71.58  nohead ls 3\n")

        self.gpfp.write("\n")

        # this is the permitted area, drawn last
        # it is simply 1/8 inch in from the edges
        # start at the lower left corner
        # right across the bottom to the lower right corner
        # this is 0.25 above the edge of the mask
        self.gpfp.write("set arrow from    3.175,  3.175 to  352.425,  3.175 nohead ls 4\n")
        # up the right side to the top
        self.gpfp.write("set arrow from  352.425,  3.175 to  352.425,261.493 nohead ls 4\n")
        # left across the top to the upper left corner
        self.gpfp.write("set arrow from  352.425,261.493 to    3.175,261.493 nohead ls 4\n")
        # down along the left side to finish at the lower left corner
        self.gpfp.write("set arrow from    3.175,261.493 to    3.175,  3.175 nohead ls 4\n")

        self.gpfp.write("\n")

    # end def Draw_LRIS_Outline()

    ########################################################################

    def Draw_DEIMOS_Outline(self):

        # this is the raw metal
        # refer to DEIMOS drawing D1114
        # start at the lower left corner
        # right along the bottom to the lower right corner
        self.gpfp.write("set arrow from  -375.36,  0     to  399.34 ,  0     nohead ls 2\n")
        # up along the right to the upper right corner
        self.gpfp.write("set arrow from   399.34,  0     to  399.34 ,229.4   nohead ls 2\n")
        # left along the top to the chopped-off corner
        self.gpfp.write("set arrow from   399.34,229.4   to -266.141,229.4   nohead ls 2\n")
        # along the chopped-off upper left corner
        self.gpfp.write("set arrow from -266.141,229.4   to -375.36 ,120.523 nohead ls 2\n")
        # down along the left to finish back at the lower left corner
        self.gpfp.write("set arrow from  -375.36,120.523 to -375.36 ,  0     nohead ls 2\n")

        self.gpfp.write("\n")

        # this is the useful area
        # refer to DEIMOS drawing D1114
        # Note also that
        # the width of the mask form at the left edge is 0.347
        # the radius of the circular mask form edge at the top is 7.7898
        # the center of the circular mask form edge at the top is 8.778 + 6.0378
        # start at the lower left corner
        # right across the bottom to the lower right corner
        self.gpfp.write("set arrow from  -366.55,7.366   to  366.55 ,  7.366 nohead ls 3\n")
        # up the right side to the upper right corner
        self.gpfp.write("set arrow from   366.55,7.366   to  366.55 ,222.96  nohead ls 3\n")
        # left across the top to the intersection with the circular mask form
        self.gpfp.write("set arrow from   366.55,222.96  to  125.019,222.96  nohead ls 3\n")
        # next 5 points are occulted by the circular arc mask form at the top
        self.gpfp.write("set arrow from  125.019,222.96  to   83.62 ,197.00  nohead ls 3\n") # to 25 degrees along arc
        self.gpfp.write("set arrow from    83.62,197.00  to   34.358,181.47  nohead ls 3\n") # to 10 degrees along arc
        self.gpfp.write("set arrow from   34.358,181.47  to       0.,178.46  nohead ls 3\n") # to  0 degrees along arc
        self.gpfp.write("set arrow from       0.,178.46  to  -34.358,181.47  nohead ls 3\n") # to 10 degrees along arc
        self.gpfp.write("set arrow from  -34.358,181.47  to   -83.62,197.00  nohead ls 3\n") # to 25 degrees along arc
        # to the circular arc mask form intersect with the top
        self.gpfp.write("set arrow from   -83.62,197.00  to -125.019,222.96  nohead ls 3\n")
        # left across the top to the chopped-off upper left corner
        self.gpfp.write("set arrow from -125.019,222.96  to  -262.86,222.96  nohead ls 3\n")
        # along the chopped-off upper left corner
        self.gpfp.write("set arrow from  -262.86,222.96  to  -366.55,118.92  nohead ls 3\n")
        # down the left side to finish at the lower left corner
        self.gpfp.write("set arrow from  -366.55,118.92  to  -366.55,  7.366 nohead ls 3\n")
        # indicate the approximate vignetting at the upper right corner
        self.gpfp.write("set arrow from   262.86,222.96  to   366.55,118.92  nohead ls 3\n")

        self.gpfp.write("\n")

        # this is the permitted area, drawn last
        # refer to DEIMOS drawing D1114
        # start at the lower left corner
        # right across the bottom to the lower right corner
        # this is 0.25 above the edge of the mask
        # this leaves room for the barcode label, and is outside of the mask form
        self.gpfp.write("set arrow from    -369.,  6.35  to  378.5  ,  6.35  nohead ls 4\n")
        # up the right side to the top
        # this is arbitrarily located outside of the mask form edge, but safely
        # inside of the cuts for the hotdog and button hole.
        self.gpfp.write("set arrow from    378.5,  6.35  to  378.5  ,223.    nohead ls 4\n")
        # left across the top to near the chopped-off corner
        # this is just more than 0.25 from the edge, but outside of the mask form
        self.gpfp.write("set arrow from    378.5,223.    to -266.141,223.    nohead ls 4\n")
        # next 2 points are the chopped-off upper left corner
        # this is 0.25 inside of the edge of the mask, and outside of the mask form
        # move because intersection with the previous top edge is not calculated
        self.gpfp.write("set arrow from  -261.65,224.91  to -370.87 ,116.03  nohead ls 4\n")
        # down along the left side to finish at the lower left corner
        # this is just barely more than 0.25 inside of the edge of the mask,
        # but outside of the mask form
        # move because intersection with the previous chopped corner is not calculated
        self.gpfp.write("set arrow from    -369.,120.523 to -369.   ,  6.35  nohead ls 4\n")

        self.gpfp.write("\n")

    # end def Draw_DEIMOS_Outline()

    ########################################################################

# end class gnuplot5()





















