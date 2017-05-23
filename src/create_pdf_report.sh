#!/bin/bash

# Create a PDF report file

if [ -z $1 ]; then
    echo "You have to specify the user stats directory"
    exit 1
fi

DIR=$1

# Convert text files into PDFs
for f in $DIR/{fan_activity,stats_overall}.txt; do
    cupsfilter $f > $f.pdf 2>/dev/null
done

# Create a final PDF
FOLDER=$(basename "`readlink -f $DIR`")
convert $DIR/{fan_activity,stats_overall}.txt.pdf $DIR/*.svg  $DIR/report-$FOLDER.pdf && \
    echo "Created $DIR/report-$FOLDER.pdf"

# Remove tmp PDF files
rm $DIR/*.txt.pdf
