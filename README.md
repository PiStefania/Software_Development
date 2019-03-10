# Software Development - Sigmod 2018

## Software Development for Information Systems

This work has been implemented for the course of Software Development for Information Systems, in Department of Informatics and Telecommunications, at University of Athens, Greece. It is based on Sigmod contest of 2018, and the Implementation is fully functional for the contest's requirements.

The **Radix Hash Join** algorithm has been implemented, using 2 hash functions, in order to create both buckets and indexes, using the **7 and 8 last bits** of each numbers, for each one of these functions, respectively.


### Designing Options
- There are comments throughout the code, for better explanation of program's functionality.
- The application is executed properly, using any number of threads.
- For more information (in greek) check [Report.pdf](https://github.com/PiStefania/Software_Development/blob/master/Report.pdf).


### Prerequisites
The application runs in **Linux**, and requires **gcc 5.4+**, **g++** and **cmake**.


### Installing - Running
1. Download and open folder **Software_Development**.
2. Open **terminal** in current folder.
3. Run `./compile.sh` and then `./runTestHarness.sh` or `./run.sh`.


## Unit Testing
For unit testing framework [CuTest](https://github.com/ennorehling/cutest) has been used.
1. Open the folder **Software_Development/cutest-1.5/**.
2. Open **terminal** in current folder.
3. Run `make` and after that `./test`.


## Contributors
- [Stefania Patsou](https://github.com/PiStefania)
- [Andreas Tsolkas](https://github.com/andreasgtech)
- [Orestis Garmpis](https://github.com/Pantokratoras7)


## Reference
- Cagri Balkesen, Jens Teubner, Gustavo Alonso, and M. Tamer Özsu. [Main-Memory
Hash Joins on Multi-Core CPUs: Tuning to the Underlying Hardware](https://ieeexplore.ieee.org/document/6544839). Proc. of the 29th
International Conference on Data Engineering (ICDE 2013) , Brisbane, Australia, April 2013.
- [Sigmod 2018](http://sigmod18contest.db.in.tum.de/task.shtml)
