masterthemainframe-wc2016
=========================

This is the analysis code and presentation I wrote for the 2016 IBM Master the
Mainframe World Championship (where I got 2nd place!)

Files
-----

* The original data, _exactly as given_, is in
`original-data/CARDUSR_CLIENT_INFO.sql.zip` and
`original-data/CARDUSR_SPPAYTB.sql.zip`.
* My own processed, easier-to-handle version of the data is in
`analysis/clients.orc` and `analysis/transactions.orc`. JSON and pgdmp versions
can be found in `analysis/other-formats`. `original-data/reformat.scala` was
used to generate these files.
* My analysis code is in `analysis/analysis.scala`, and its output in
`analysis/analysis-output.txt`.
* The source for the graphs is in `presentation/img-src/graphs.xlsx`. You may
need [this font](https://sourceforge.net/projects/cm-unicode/).
* My presentation is in `presentation/presentation.pdf`.

License ![License](http://img.shields.io/:license-mit-blue.svg)
-------

The licences for the data (`.sql.zip`, `.orc`, `.json.zip`, and `.pgdmp` files),
and most icons in `presentation/img` are unknown. Use at your own risk.

For everything else:

    The MIT License (MIT)

    Copyright (c) 2016 Stephen

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
