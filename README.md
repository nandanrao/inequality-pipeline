# Inequality Pipeline

## Introduction

This repo consists of the primary part of the data processing pipeline built for a couple of projects regarding calculating levels of inequality from satelite imagery. This is a Scala/Spark application, which is currently being run on AWS and now on Marenostrum via their custom Spark framework. It relies very heavily on [Geotrellis](https://github.com/locationtech/geotrellis), a spark geoprocessing framework.

The rest of the readme will be ordered "chronologically", documenting the steps required to go from data to calculations. I will try to highlight where work is most needed, and give some logic as to why things are structured the way they are, but there will naturally be some gaps. The codebase is quite small, and after reading this things should be fairly clear.

## Input Data

Everything we do comes from 2 separate sources of satellite imagery, both provided in raster format: one is satellite imagery of nightlight with the level of luminosity encoded from 0 to 63, the other is a constructed raster file with an estimate of the population living in the space of every pixel. There are two sources of population data, with different resolutions/origins/etc. The nightlight data also changes through time, with older files having a lower resolution.

All of these files are stored on Marenostrum's distributed file system, and currently on AWS S3 as well. I have not run the majority of this pipeline with anything but 2012/2013 data from nightlight and 2013 population data from the Landsat project.

The other type of data is best thought of as vector data (shapefiles, for instance): the areas in which we wish to aggregate data in the inequality calculations. Ideally this should be injested at-run-time, so the calculations can be run on any shapefile. Currently, we can injest either rasters or shapefiles, but have been testing primarily with rasters (this is TODO, more on this in the IO section).

## Preprocessing / Resampling

The nightlight and the population rasters do not have the same origins, and as such, we cannot simply place them on top of one another without preprocessing first. The basic idea is to create a process that will work with all raster images: resample all of them onto one common grid. Essentially this looks like:

* Define the common grid (an origin, extents, and pixel size). For robustness, one should probably pick a resolution that is higher than the resolution of any of the inputs, so that we are upsampling, and losing as little information as is reasonable.

* Use GDAL to resample the different rasters onto this grid. The docker container for GDAL is great and running it through its CLI is simple and seems to scale reasonably well. You will need to choose a different resampling scheme for the different data types: nightlight should be sampled as an image, i.e. bicubic interpolation, while the population will need to use something like nearest-neighbors (after which you will need to scale the value by multiplying by (target-pixel-area/source-pixel-area)).

Note that this area of the pipeline has NOT been formalized/built yet. I have simply run this ad-hoc for a few of the files, and have been working on those. This should be formalized somehow. It is tempting to do it on-demand in Geotrellis, but after some investigation it proved slower and buggier than simply using GDAL as a separate stage. I encourage further research into how to do this, however.

## Some Scripts for Preprocessing

#### Rasterizing a vector format.

* -a gives the layer you want to turn into the raster value
* -tr gives the pizel size
* -te gives the extent
```{sh}
gdal_rasterize -a ADM0_CODE -tr 0.0083333 0.0083333 path/to/countries.shp -te __ __ __ __ /path/to/countries.tif
```

#### Resampling

To resample, use gdalwarp.
* -ot target type --
```{sh}
gdalwarp -tr SIZE SIZE -ot Float32 -te __ __ __ __ /path/to/infile.tif /path/to/outfile.tif
```

#### Reformatting
```{sh}
gdal_translate /path/to/infile.adf /path/to/outfile.tif
```

## I/O

Take a look at the IO object. Here you can see we were originally supporting S3, and have since begun the transition to Marenostrum's GPFS.

Currently shapefiles for aggregation areas are first being converted to GeoTiff format onto the same grid. There is some code in the IO class for reading shapefiles directly, and you will see in the code that you can choose the input as raster or vector, but the vector reading has not been updated to run on Marenostrum. This should be done, and tested so that this can be provided in shape rather than raster format.

## Wealth Rasters

The first part of generating the calculations is to create what can be considered an "intermediate artifact": a 2-band raster file in which one band is some proxy for wealth-per-population in that pixel (for example nightlight/population, possibly with some othe transformations), while the second band is simply the population.

The Wealth object has a main function that creates these wealth rasters and writes them into a GeoTif format, which is extremely valuable for considering the problems inherent with these data sources. For the research itself, the ability to create a wealth raster which adequately reflects wealth-per-population is crucial and non-trivial given the problems with the data. Once we have wealth rasters, the rest of the pipeline is much more stable: it consists of running calculations, of which more can be expected to be added, but which in general should be fairly stable.

One major challenge has to do with top and bottom coding of the data: nightlight peaks at a value of 63, while wealth, and population, have no upper limit. Similarly, pixels with populations of under 1 should not be used in the calculation, but also maybe those with less then 2, or 3, or...?

Currently, the only implemented methods for handling this are "crushing" the bottom (choosing the population level below which we throw out the pixel) and "topcoding" the population rasters (so that they also have an upper-truncated value: a very ad-hoc method, only useful for observing the affects of this problem).

## Grouping By Shapes

The first step in the calculations is grouping the data by the shape.

A major performance choice: do we aggregate data of one shape into to the same partition, and run calculations in parallel, or balance them evenly accross partitions, and run each step of the calculation in parallel? Currently, I implemented the latter, which is naturally very robust to extremely large and uneven sized shapes. This may or may not be the correct choice.

The GroupByShape module provides the tools for grouping both when given a raster file for the shapes (currently used), and when given a vector file for the shapes.

## Gini / Growth Calculations

Gini and Growth are the two main objects for the two main calculations. Both have a main class with a shit-ton of CLI arguments that should really be moved into a config file. Both also have a lot of shared code in that CLI parsing and prepping that should be moved somewhere.

Both follow the same basic process: read the raster files, create wealth rasters, group the data in the wealth rasters by the provided shapes, create an RDD with those values, then loop through the shapes and run the calculation on each shape, and then print out a CSV of the calculations at the end.

## Tests

Most of the test coverage is focused on the Gini/Growth calculations doing what we want, as they are simply mathematical calculations that by nature fail silently, this is important to build on, in my opinion at least, as it's nothing more than a start!
