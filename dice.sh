#!/bin/bash
if [ $# -ne 2 ]; then
    echo "Usage: dice.sh inp.file out.file"
    exit 
fi
echo ${1}
rm -f ${2}
cat ${1} | grep "Current count" | sed 's/^Current.*, row: //' > ${2}
