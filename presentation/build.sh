#!/bin/sh

rm *.aux *.log *.nav *.out *.snm *.toc *.pdf

pdflatex presentation.tex
pdflatex presentation.tex

rm *.aux *.log *.nav *.out *.snm *.toc
