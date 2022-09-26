#!/bin/sh

for file in *.pdf
do
    pdfcrop $file
    mv *crop.pdf $file
done
