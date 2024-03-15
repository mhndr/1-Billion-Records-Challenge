
#attempt 1
cc -g -DDEBUG ./main.c ./str_split.c ./ring_buffer.c -o ./1brc 
/usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" 1brc.entitlements
codesign -s - -f --entitlements 1brc.entitlements ./1brc
ulimit -c unlimited
#sudo sysctl kern.coredump=1

#attempt 2
cc ./main2.c ./hashtable.c -o ./1brc_2


