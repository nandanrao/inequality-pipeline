#!/bin/sh

SIZE=$1
OUTDIR=$2

for YEAR in "2011" #"2015" # "2011" "2012" "2013" "2014" "2015"
do 
  gdal_rasterize -burn 1 -tr $SIZE $SIZE -l "Global_3G_${YEAR}12" "Mobile Coverage Explorer WGS84 ${YEAR}12 - ESRI SHAPE/Data/Global_3G_${YEAR}12.shp" "${OUTDIR}/${YEAR}.tif"
done


