# go-reconcile

A set reconciliation library for data synchronization and de-duplication. This library is written to be able to
interface with [oftn-oswg/ts-reconcile](https://github.com/oftn-oswg/ts-reconcile), the TypeScript/JavaScript implementation.

The algorithms implemented here are based on:

**David Eppstein**, **Michael T. Goodrich**, **Frank Uyeda**, and **George Varghese**. 2011. _What's the difference?: efficient set reconciliation without prior context._ In Proceedings of the ACM SIGCOMM 2011 conference (SIGCOMM '11). ACM, New York, NY, USA, 218-229. DOI: https://doi.org/10.1145/2018436.2018462
