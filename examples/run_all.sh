#!/bin/bash -e

python3 SimpleIterator.py -s start.gro
echo TopPO4 | python3 Density.py -s start.gro -n
python3 AngleWithZ-fixedgroups.py -s start.gro
echo 3 4 | python3 AngleWithZ-simplified.py -s start.gro -n index.ndx
echo 3 4 | python3 AngleWithZ-multgroups.py -s start.gro -n index.ndx
echo 3 4 | python3 AngleWithZ-ndxgroups.py -s start.gro -n index.ndx
echo 3 4 | python3 AngleWithZ-ndxgroupsParallel.py -s start.gro -n index.ndx
echo 3 4 | python3 AngleWithZ-ndxgroupsParallel_noargparse.py -s start.gro
echo 1 2 3 | python3 CoordinatePropExtraction.py -s start.gro 

rm -f dens.png dens.eps data.xvg
