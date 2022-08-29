#/bin/bash

awk '{
    print $7, $12, $17, $20
    }' $1 | awk -F"ms" '{
        print $1
        }' | awk '{
                    if ($0 ~ /^[0-9]/) {
                        etime += $4;
                        count++;
                        if (set == 0) {
                            batchLength = $1
                            keySize = $2
                            valueSize = $3
                            totalSize = (keySize + valueSize) * batchLength
                            set = 1
                        }
                    } else {
                        if (count > 0)
                            print batchLength, keySize, valueSize, totalSize/(1024), "KB", etime/count;
                        etime = 0;
                        count = 0;
                        set = 0;
                    }
                }'
