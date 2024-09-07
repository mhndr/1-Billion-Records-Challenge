python3 ./create_measurements.py 1000000 
sh ./build.sh
time ./main2 measurements.txt #>output
echo $? 

python3 ./create_measurements.py 1000000000
sh ./build.sh
time ./main2 measurements.txt #>output
echo $? 
