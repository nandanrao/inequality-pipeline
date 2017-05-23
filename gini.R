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
