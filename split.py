import os
import csv
import sys
if not os.path.exists('./split'):
    os.makedirs('split')
if len(sys.argv)<2:
    print("Usage: split.py NUMBER, where NUMBER is the maximum number of link the file should have")
    sys.exit()
maxlines=int(sys.argv[1])
print(f"Max Lines={maxlines}")


for filename in (os.listdir()):
    file_index=0
    if filename.endswith('.csv'):
        with open(filename,'r') as fp:
            linecount=0
            for line in fp:

                with open(os.path.join('split',filename.split('.csv')[0]+'_'+str(linecount//maxlines)+'.csv'),'a') as fp_write:
                    fp_write.write(line) 
                linecount+=1
        print(f'Splitted {filename}: {linecount} lines in {linecount//maxlines} files.')
                    
