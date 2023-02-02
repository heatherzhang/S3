import boto3, threading
from datetime import datetime
from random import randrange
from airflow.providers.postgres.hooks.postgres import PostgresHook
# import gzip # if needed


bucket = 'your bucket name'
thread_cnt = 10
test_per_thread = 100
db_conn = PostgresHook(postgres_conn_id="OPDB_DEV").get_conn()


# folder/file for testing
s3_file_list = [
'some_folder1/0004000091.json',
'some_folder1/0004000096.json',
'some_folder1/0004000113.json',
'some_folder1/0004000119.json',
'some_folder1/0004000129.json',
'some_folder1/0004000170.json',
'some_folder1/0004000180.json',
'some_folder1/0004000224.json',
'some_folder1/0004000232.json',
'some_folder1/0004000245.json',
'some_folder2/0004000055.json',
'some_folder2/0004000124.json',
'some_folder2/0004000172.json',
'some_folder2/0004000203.json',
'some_folder2/0004000218.json',
'some_folder2/0004000229.json',
'some_folder2/0004000242.json',
'some_folder2/0004000250.json',
'some_folder2/0004000267.json',
'some_folder2/0004000311.json',
'some_folder3/0001000001.json',
'some_folder3/0004000012.json',
'some_folder3/0004000019.json',
'some_folder3/0004000026.json',
'some_folder3/0004000139.json',
'some_folder3/0004000187.json',
'some_folder3/0004000223.json',
'some_folder3/0004000228.json',
'some_folder3/0004000424.json',
'some_folder3/0004000459.json',
'some_folder4/0004000006.json',
'some_folder4/0004000025.json',
'some_folder4/0004000039.json',
'some_folder4/0004000066.json',
'some_folder4/0004000160.json',
'some_folder4/0004000189.json',
'some_folder4/0004000199.json',
'some_folder4/0004000292.json',
'some_folder4/0004000350.json',
'some_folder4/0004000410.json',
'some_folder5/0004000010.json',
'some_folder5/0004000099.json',
'some_folder5/0004000190.json',
'some_folder5/0004000239.json',
'some_folder5/0004000246.json',
'some_folder5/0004000285.json',
'some_folder5/0004000286.json',
'some_folder5/0004000395.json',
'some_folder5/0004000399.json',
'some_folder5/0004000438.json',
'some_folder6/0004000007.json',
'some_folder6/0004000015.json',
'some_folder6/0004000030.json',
'some_folder6/0004000078.json',
'some_folder6/0004000085.json',
'some_folder6/0004000103.json',
'some_folder6/0004000135.json',
'some_folder6/0004000166.json',
'some_folder6/0004000186.json',
'some_folder6/0004000191.json',
'some_folder7/0004000001.json',
'some_folder7/0004000054.json',
'some_folder7/0004000064.json',
'some_folder7/0004000081.json',
'some_folder7/0004000090.json',
'some_folder7/0004000152.json',
'some_folder7/0004000213.json',
'some_folder7/0004000261.json',
'some_folder7/0004000270.json',
'some_folder7/0004000389.json',
'some_folder8/0001000002.json',
'some_folder8/0004000021.json',
'some_folder8/0004000029.json',
'some_folder8/0004000052.json',
'some_folder8/0004000053.json',
'some_folder8/0004000093.json',
'some_folder8/0004000110.json',
'some_folder8/0004000118.json',
'some_folder8/0004000136.json',
'some_folder8/0004000153.json',
'some_folder9/0004000069.json',
'some_folder9/0004000157.json',
'some_folder9/0004000193.json',
'some_folder9/0004000258.json',
'some_folder9/0004000269.json',
'some_folder9/0004000320.json',
'some_folder9/0004000362.json',
'some_folder9/0004000403.json',
'some_folder9/0004000464.json',
'some_folder9/0004000467.json',
'some_folder10/0004000033.json',
'some_folder10/0004000137.json',
'some_folder10/0004000156.json',
'some_folder10/0004000173.json',
'some_folder10/0004000214.json',
'some_folder10/0004000237.json',
'some_folder10/0004000257.json',
'some_folder10/0004000307.json',
'some_folder10/0004000308.json',
'some_folder10/0004000312.json',
'some_folder11/0004000062.json',
'some_folder11/0004000106.json',
'some_folder11/0004000107.json',
'some_folder11/0004000130.json',
'some_folder11/0004000167.json',
'some_folder11/0004000196.json',
'some_folder11/0004000198.json',
'some_folder11/0004000234.json',
'some_folder11/0004000305.json',
'some_folder11/0004000317.json',
'some_folder12/0004000037.json',
'some_folder12/0004000047.json',
'some_folder12/0004000050.json',
'some_folder12/0004000074.json',
'some_folder12/0004000089.json',
'some_folder12/0004000114.json',
'some_folder12/0004000168.json',
'some_folder12/0004000184.json',
'some_folder12/0004000221.json',
'some_folder12/0004000222.json',
'some_folder13/0001000000.json',
'some_folder13/0004000003.json',
'some_folder13/0004000016.json',
'some_folder13/0004000023.json',
'some_folder13/0004000024.json',
'some_folder13/0004000036.json',
'some_folder13/0004000040.json',
'some_folder13/0004000065.json',
'some_folder13/0004000092.json',
'some_folder13/0004000097.json',
'some_folder14/0004000009.json',
'some_folder14/0004000020.json',
'some_folder14/0004000111.json',
'some_folder14/0004000209.json',
'some_folder14/0004000219.json',
'some_folder14/0004000282.json',
'some_folder14/0004000283.json',
'some_folder14/0004000293.json',
'some_folder14/0004000367.json',
'some_folder14/0004000370.json',
'some_folder15/0004000005.json',
'some_folder15/0004000088.json',
'some_folder15/0004000117.json',
'some_folder15/0004000121.json',
'some_folder15/0004000181.json',
'some_folder15/0004000204.json',
'some_folder15/0004000216.json',
'some_folder15/0004000238.json',
'some_folder15/0004000289.json',
'some_folder15/0004000324.json',
'some_folder16/0004000044.json',
'some_folder16/0004000056.json',
'some_folder16/0004000071.json',
'some_folder16/0004000075.json',
'some_folder16/0004000080.json',
'some_folder16/0004000098.json',
'some_folder16/0004000100.json',
'some_folder16/0004000104.json',
'some_folder16/0004000158.json',
'some_folder16/0004000197.json',
'some_folder17/0004000032.json',
'some_folder17/0004000126.json',
'some_folder17/0004000128.json',
'some_folder17/0004000147.json',
'some_folder17/0004000225.json',
'some_folder17/0004000226.json',
'some_folder17/0004000243.json',
'some_folder17/0004000244.json',
'some_folder17/0004000277.json',
'some_folder17/0004000315.json',
'some_folder18/0000000001.json',
'some_folder18/0004000014.json',
'some_folder18/0004000178.json',
'some_folder18/0004000294.json',
'some_folder18/0004000316.json',
'some_folder18/0004000323.json',
'some_folder18/0004000361.json',
'some_folder18/0004000404.json',
'some_folder18/0004000427.json',
'some_folder18/0004000437.json'
]
s3_file_list_cnt = len(s3_file_list)

# for a single thread, it downloads thread_file_cnt of files, from a random file in the whole file list, using s3 client for the thread
def read_from_s3_thread(thread_files_cnt, s3, total_files_cnt):
    for p in range(0, thread_files_cnt):
        provider_file_ind = randrange(1, total_files_cnt)
        file_name = provider_list[provider_file_ind]

        s = datetime.now()
        obj = s3.Object(bucket, file_name)
        data=obj.get()['Body'].read()
# if file needs uncompress, this will add to timing
#         data = gzip.decompress(obj.get()['Body'].read())
#         print(datetime.now()-s, "====", len(data), "=====", (data.decode('ascii'))[0:50])
        print("elapsed:", datetime.now()-s, "     ==len(json)==", len(data))


# perform test by spin off thread_cnt of threads, files_per_thread of files for each thread, for the entire file_list_cnt of files, with s3 client
def read_all(thread_cnt, files_per_thread, files_total_cnt, s3):
    index_threads = []

    for t in range(0, thread_cnt):
        process = (
            threading.Thread(
                target=read_from_s3_thread,
                args=(files_per_thread, s3, files_total_cnt)
            )
        )
        process.start()
        index_threads.append(process)

    print('read json from s3 start: ', datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))

    for i in range(0, thread_cnt):
        index_threads[i].join()

    print('read json from s3 end: ', datetime.today().strftime("%m/%d/%Y, %H:%M:%S"))


# main test
s3_session = boto3.Session()
s3 = s3_session.resource('s3')
    
read_all(thread_cnt, test_per_thread, s3_file_list_cnt, s3)
