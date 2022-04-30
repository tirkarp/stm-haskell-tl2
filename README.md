# Transactional Locking II (TL2) in STM Haskell

This is an implementation of TL2 [_(Dice, Shalev and Shavit)_](https://dcl.epfl.ch/site/_media/education/4.pdf) in STM Haskell. Code adapted from implementation of [_Du Bois_](https://link-springer-com.ezproxy.lib.purdue.edu/content/pdf/10.1007/978-3-642-22045-6.pdf). 

Included in the `src` directory are sample concurrent algorithms and data structures designed to use STM, adapted from:
- [stm-data-collection](https://hackage.haskell.org/package/stm-data-collection-0.1.0.0/candidate/docs/Data-STM-PriorityQueue-Class.html#t:PriorityQueue)
- [stm-linkedlist](https://hackage.haskell.org/package/stm-linkedlist-0.1.0.0/docs/Data-STM-LinkedList.html)
- [_@jasonincanada_](https://github.com/jasonincanada/stm-haskell)

### Benchmarks

Performance benchmark in this project is performed with [`criterion`](https://hackage.haskell.org/package/criterion) on a producer-consumer problem with 100,000 shared elements on a binary heap, with 16 producers and 16 consumers. Performance report of this lock-based TL2 algorithm is found in `lock-based result.html`. Performance report of the default lock-free STM is found in `lock-free result.html`.

### Usage

Make sure you have [`stack`](https://haskellstack.org/) installed. Clone this repository. Then run `stack build; stack execute stm-exe`. To generate the report again, append the flag `-- --output report_filename.html` at the end of the command.
