# Pipeline


## Random TODO
* Deal with weird nonkey in municipality ids
* Deal with leftouterjoin when there is no nl or pop for the shape. (test!)
* memory leaks??? Hadoop file reader???



## Gini Calcs
* Figure out why the hell the gini for these SA municipalities is so bogus:
```{Scala}
List(NaN, 0.9995170231796209, NaN, NaN, NaN, 0.9862122443726093, NaN, 0.9542968885300525, NaN, 0.9996139917057718, 0.9754872257367708, NaN, 0.9991289701089485, NaN, NaN, NaN, NaN, 0.9811295670275513, 0.9987674014174956, 0.9998780050831613, NaN, NaN, NaN, 0.9974416880578758, 0.9995648663038992, NaN, NaN, NaN, 0.977375072651057, 0.9696784485695673, 0.989838473255034)
```

* Refactor for submission and inputs and run at scale

## ETL 
* Try with S3 backend
* Run with all nightlight and pop (EMR cluster, figure out size)

## 
