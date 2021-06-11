q kdb-tick/tick.q sym OnDiskDB -p 5000

q kdb-tick/tick/r.q localhost:5000 localhost:5002 -p 5001

quote:([]time:`timespan$();sym:`symbol$();bid:`float$();ask:`float$();bsize:`int$();asize:`int$())
trade:([]time:`timespan$();sym:`symbol$();price:`float$();size:`int$())

