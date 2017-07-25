gini <- function(x, weights=rep(1,length=length(x))){
        ox <- order(x)
        x <- x[ox]

        # zip and sort
        weights <- weights[ox]/sum(weights)

        # cumsum weights
        p <- cumsum(weights)

        # zip, map, cumsum
        nu <- cumsum(weights*x)

        # normalize nu by sum...
        n <- length(nu)
        nu <- nu / nu[n]

        # filter RDD's by index (zip with index first)
        # zip and reduce _ + _ on both separate
        sum(nu[-1]*p[-n]) - sum(nu[-n]*p[-1])
}

# SCALA TESTS
gini(rep(1, 10), 1:10) # 0.0
gini(c(1,1,5,5,10,10), c(10,10,1,1,10,10)) # 0.4099379
gini(c(0.00001,1,5,5,10,10), c(1000,10,1,1,10,10)) # 0.9781078
