# Pipeline

* Turn vectors of whatever shapes into rasters into spatial RDD
* That spatial RDD should be tiled the same as our input data RDDs!!!!
* Join the spatial shapes with the associated other spatial rdds into a (K, Seq) RDD
* Group-by-key into single executor and then reduce into a matrix (per key) and do calculations on that matrix (in executor?) (or just reduce by key?? Is probably the same in the end.)
* Print results
