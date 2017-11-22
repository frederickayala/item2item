NAME=$1
rm -f $NAME.out
(ionice -c2 nice -n 11 nohup ./$NAME 2>$NAME.err 1>$NAME.out) &
echo $! >> $NAME.out
