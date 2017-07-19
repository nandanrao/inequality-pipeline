#!/bin/sh



# translate ADF to TIF keeping resolution/extent the same
gdal_translate -a_srs EPSG:4326 landscan/w001001.adf landscan/w001001.tif

# then warp all 3 tifs to new extent and resolution with different resampling:
# "near" or "cubicspline"

# get xmin, ymin, etc...
gdalwarp -te 30.15 -31.15 30.25 -31.05 -ot Float32 nightlight/2013.tif mini-rasters/2013.tif


gdalwarp -te -180 -60 180 85 -tr 0.004166666666667 0.004166666666667 -ot Float32 out/landscan-2013-raw.tif out/pop-2013.tif

gdalwarp -te -180 -90 180 90 -tr 0.008333333333330 0.008333333333330 -ot Float32 out/landscan-2013-raw.tif out/simple/pop-2013.tif

gdalwarp -te -180 -90 180 90 -tr 0.008333333333330 0.008333333333330 -ot Float32 -r cubicspline
# then divide pop tif by 4... :(
