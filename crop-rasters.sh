#!/bin/sh

OUTDIR=$1

for YEAR in "2008" "2009"
do
  gdalwarp -cutline sa.shp -crop_to_cutline "nightlight/F16${YEAR}.v4b_web.stable_lights.avg_.tif" "nightlight/${YEAR}.tif"
done

for YEAR in "2010" "2011" "2012" "2013"
do
  gdalwarp -cutline sa.shp -crop_to_cutline "/gpw-v4-population-count_${YEAR}.tif" "nightlight/${YEAR}.tif"
done
