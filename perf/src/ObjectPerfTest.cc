/*
 * Copyright 2009-2017 Alibaba Cloud All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <alibabacloud/oss/OssClient.h>
#include "Config.h"
//#include "../Utils.h"
#include <fstream>
#include <src/utils/Utils.h>
#include <src/utils/FileSystemUtils.h>
#include <chrono>

namespace AlibabaCloud {
namespace OSS {

struct PCount {
    int64_t success;
    int64_t failed;
    int64_t total;
    mutable std::mutex mtx;
    mutable std::condition_variable cv;
    mutable bool ready;
};

bool g_sync;

class PutObjectAsyncContex : public AsyncCallerContext
{
public:
    PutObjectAsyncContex(PCount *pcount) {
        //pcount->failed = 0;
        //pcount->success = 0;
        this->pcount = pcount;
    }
    virtual ~PutObjectAsyncContex()
    {
    }


    mutable PCount* pcount;
};

void PutObjectHandler(const AlibabaCloud::OSS::OssClient* client,
    const PutObjectRequest& request,
    const PutObjectOutcome& outcome,
    const std::shared_ptr<const AsyncCallerContext>& context)
{
    (void)request;
    (void)outcome;
    (void)client;
    //std::cout << "Client[" << client << "]" << "PutObjectHandler, tag:" << context->Uuid() << 
    //    ", key:" << request.Key() << std::endl;
    if (context != nullptr) {
        auto ctx = static_cast<const PutObjectAsyncContex *>(context.get());
        //EXPECT_EQ(outcome.isSuccess(), true);

        std::unique_lock<std::mutex> lck(ctx->pcount->mtx);
        if (outcome.isSuccess() == true) {
            ctx->pcount->success++;
        } else {
            ctx->pcount->failed++;
        }
        if (ctx->pcount->success + ctx->pcount->failed == ctx->pcount->total) {
            ctx->pcount->ready = true;
            ctx->pcount->cv.notify_all();
        }

    }
}


std::shared_ptr<std::iostream> GetRandomStream(int length)
{
    std::shared_ptr<std::stringstream> stream = std::make_shared<std::stringstream>();
    std::stringstream ss;
    for (int i = 0; i < length; i++) {
        *stream << static_cast<char>('!' + rand() % 90);
    }
    stream->seekg(0);
    stream->seekp(0, std::ios_base::end);
    return stream;
}

std::string get_key(int cidx,int idx) {
    char buf[65];
    sprintf(buf, "%d%063d",cidx, idx);
    buf[64]='\0';
    return std::string(buf);
}




#define TSIZE 2048
#define BSIZE 2048
std::string d[TSIZE][BSIZE];
std::mutex g_mtx;
int g_mill;

std::shared_ptr<std::iostream> g_data[TSIZE][1024];


std::string query_md5key(int cidx, int idx) {
    return d[cidx][idx];
}
void init_key() {

    for (int i=0; i <TSIZE; i++) {
        for (int j=0; j <BSIZE;j++) {
            std::string org = get_key(i,j);
            std::string md5 = ComputeContentMD5(org.c_str(), org.size());
            //std::cout<< md5<<std::endl;
            d[i][j]=md5;
        }
    }
}

void put_test(std::string bucketname, int startidx, int size)
{
    std::cout <<"start idx: "<< startidx<<std::endl;

    struct PCount pcount;
    pcount.total = size;
    pcount.ready = false;

    /*
    std::shared_ptr<std::iostream> data[4096];
    for (int i = 0; i < 4096; i++) {
        data[i] =GetRandomStream(4096);
    }*/
    ClientConfiguration conf;
    conf.maxConnections = 16;
    conf.enableCrc64 = false;


    std::shared_ptr<OssClient> Client;
    Client = std::make_shared<OssClient>(Config::Endpoint, Config::AccessKeyId, Config::AccessKeySecret, conf);

    auto start = std::chrono::high_resolution_clock::now();
    for (int i=0; i < size; i++) {
        std::string memKey = query_md5key(startidx,i); 
        auto memContent = g_data[startidx][i % 1024];
        if (g_sync) {
            auto pOutcome = Client->PutObject(bucketname, memKey, memContent);
            if (pOutcome.isSuccess() == true) {
                pcount.success++;
            } else {
                pcount.failed++;
            }
            //EXPECT_EQ(pOutcome.isSuccess(), true);
        } else {
            std::shared_ptr<PutObjectAsyncContex> memContext = std::make_shared<PutObjectAsyncContex>(&pcount);
            memContext->setUuid(memKey);
            PutObjectAsyncHandler handler = PutObjectHandler;
            PutObjectRequest memRequest(bucketname, memKey, memContent);
            Client->PutObjectAsync(memRequest, handler, memContext);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> tm = end - start;	// 毫秒
    {
        std::unique_lock<std::mutex> lck(g_mtx,std::defer_lock);
        lck.lock();
        std::cout << "thread: "<< startidx <<" time spend request: " << tm.count() << "ms" << std::endl;
        g_mill += tm.count();
        lck.unlock();
        if (g_sync) {
            std::cout << "thread: "<< startidx <<" success: " << pcount.success << " failed" << pcount.failed << std::endl;
            return;
        }
    }
    //std::cout << "is there a" <<std::endl;
    {
        std::unique_lock<std::mutex> lck(pcount.mtx);
        if (!pcount.ready) pcount.cv.wait(lck);
    }

    //std::cout << "is there b" <<std::endl;
    end = std::chrono::high_resolution_clock::now();
    tm = end - start;	// 毫秒
	// std::chrono::duration<double, std::micro> tm = end - start; 微秒
    {
        std::unique_lock<std::mutex> lck(g_mtx,std::defer_lock);
        lck.lock();
        std::cout << "thread: "<< startidx <<" time spend done: " << tm.count() << "ms" << std::endl;
        lck.unlock();
    }

    /*
    std::string memKey = TestUtils::GetObjectKey("PutObjectAsyncBasicTest");
    auto memContent = TestUtils::GetRandomStream(102400);

    std::string fileKey = TestUtils::GetObjectKey("PutObjectAsyncBasicTest");
    std::string tmpFile = TestUtils::GetTargetFileName("PutObjectAsyncBasicTest").append(".tmp");
    TestUtils::WriteRandomDatatoFile(tmpFile, 1024);
    auto fileContent = std::make_shared<std::fstream>(tmpFile, std::ios_base::out | std::ios::binary);

    PutObjectRequest fileRequest(BucketName, fileKey, fileContent);
    std::shared_ptr<PutObjectAsyncContex> fileContext = std::make_shared<PutObjectAsyncContex>();
    fileContext->setUuid("PutobjectasyncFromFile");

    Client->PutObjectAsync(memRequest, handler, memContext);
    Client->PutObjectAsync(fileRequest, handler, fileContext);
    std::cout << "Client[" << Client << "]" << "Issue PutObjectAsync done." << std::endl;

    {
        std::unique_lock<std::mutex> lck(fileContext->mtx);
        if (!fileContext->ready) fileContext->cv.wait(lck);
    }

    {
        std::unique_lock<std::mutex> lck(memContext->mtx);
        if (!memContext->ready) memContext->cv.wait(lck);
    }

    fileContent->close();

    EXPECT_EQ(Client->DoesObjectExist(BucketName, memKey), true);
    EXPECT_EQ(Client->DoesObjectExist(BucketName, fileKey), true);

    fileContext = nullptr;

    TestUtils::WaitForCacheExpire(1);
    EXPECT_EQ(RemoveFile(tmpFile), true);
    */
}


class GetObjectAsyncContex : public AsyncCallerContext
{
public:
    GetObjectAsyncContex(PCount *pcount) {
        this->pcount = pcount;
    }
    ~GetObjectAsyncContex() {}
    mutable std::mutex mtx;
    mutable std::condition_variable cv;
    mutable std::string md5;
    mutable bool ready;
    mutable PCount* pcount;
};



void GetObjectHandler(const AlibabaCloud::OSS::OssClient* client, 
                      const GetObjectRequest& request, 
                      const GetObjectOutcome& outcome, 
                      const std::shared_ptr<const AsyncCallerContext>& context)
{
    (void)client;
    (void)request;
    //std::cout << "Client[" << client << "]" << "GetObjectHandler" << ", key:" << request.Key() << std::endl;
    if (context != nullptr) {
        auto ctx = static_cast<const GetObjectAsyncContex *>(context.get());
        //EXPECT_EQ(outcome.isSuccess(), true);


        //std::string memMd5 = ComputeContentMD5(*outcome.result().Content().get());
        //ctx->md5 = memMd5;
        std::unique_lock<std::mutex> lck(ctx->mtx);
        if (outcome.isSuccess() == true) {
            ctx->pcount->success++;
        } else {
            ctx->pcount->failed++;
        }
        if (ctx->pcount->success + ctx->pcount->failed == ctx->pcount->total) {
            ctx->pcount->ready = true;
            ctx->pcount->cv.notify_all();
        }
        //ctx->ready = true;
        //ctx->cv.notify_all();
    }
}

void get_test(std::string bucketname,int startidx, int size)
{
    struct PCount pcount;
    pcount.total = size;
    pcount.ready = false;
    pcount.failed = 0;
    pcount.success = 0;

    std::shared_ptr<OssClient> Client;
    Client = std::make_shared<OssClient>(Config::Endpoint, Config::AccessKeyId, Config::AccessKeySecret, ClientConfiguration());

    GetObjectAsyncHandler handler = GetObjectHandler;


    auto start = std::chrono::high_resolution_clock::now();
    for (int i=0; i < size; i++) {
        std::string memKey = query_md5key(startidx,i); 
        GetObjectRequest request(bucketname, memKey);
        if (g_sync) {
              auto outome   = Client->GetObject(bucketname, memKey);
              //EXPECT_EQ(outome.isSuccess(), true);
              if (outome.isSuccess() == true) {
                  pcount.success++;
              } else {
                  pcount.failed++;
              }
        } else {
            std::shared_ptr<GetObjectAsyncContex> memContext = std::make_shared<GetObjectAsyncContex> (&pcount);
            Client->GetObjectAsync(request, handler, memContext);
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> tm = end - start;	// 毫秒
    {
        std::unique_lock<std::mutex> lck(g_mtx,std::defer_lock);
        lck.lock();
        g_mill += tm.count();
        std::cout <<"thread: "<<startidx << " spend time request: " << tm.count() << "ms" << std::endl;
        lck.unlock();
        if (g_sync) {
            std::cout << "thread: "<< startidx <<" success: " << pcount.success << " failed" << pcount.failed << std::endl;
            return;
        }
    }

    {
        std::unique_lock<std::mutex> lck(pcount.mtx);
        if (!pcount.ready) pcount.cv.wait(lck);
    }

    end = std::chrono::high_resolution_clock::now();
    tm = end - start;	// 毫秒
	// std::chrono::duration<double, std::micro> tm = end - start; 微秒
    {
        std::unique_lock<std::mutex> lck(g_mtx,std::defer_lock);
        lck.lock();
        std::cout << "thread: "<<startidx<< " spend time done: " << tm.count() << "ms" << std::endl;
        lck.unlock();
    }
}

} //oss
} //ali

typedef   struct thread_info {    /* Used as argument to thread_start() */
        pthread_t thread_id;        /* ID returned by pthread_create() */
        int       thread_idx;       /* Application-defined thread # */
        int       size;
} tinfo_t;

void *perf_put(void *arg) {

    tinfo_t *info = (tinfo_t*)arg;
    AlibabaCloud::OSS::put_test("lvwjtest",info->thread_idx,info->size);
    return nullptr;
}
void *perf_get(void *arg) {
    tinfo_t *info = (tinfo_t*)arg;
    AlibabaCloud::OSS::get_test("lvwjtest",info->thread_idx,info->size);
    return nullptr;
}

typedef void *(*fn)(void*);



void perf_run(char *type, int threadnum, int size)
{
    void *res;
    tinfo_t tinfo[TSIZE];
    pthread_attr_t attr;
    fn func;

    pthread_attr_init(&attr);
    if (0 == strcmp(type,"put")) {
        func = perf_put;
    } else {
        func = perf_get;
    }

    for (int i = 0; i < threadnum; i++) {
        tinfo[i].thread_idx = i;
        tinfo[i].size = size;
        pthread_create(&tinfo[i].thread_id, &attr,
                              func, &tinfo[i]);
    }

    for (int i = 0; i < threadnum; i++) {
        pthread_join(tinfo[i].thread_id, &res);
        //free(res);
    }
    std::cout << "avg g_mil: "  <<AlibabaCloud::OSS::g_mill / threadnum <<std::endl;
}


int main(int argc, char *argv[]) {


    /*
    std::shared_ptr<std::iostream> data[4096];
    for (int i = 0; i < 4096; i++) {
        data[i] =GetRandomStream(4096);
    }*/

    AlibabaCloud::OSS::g_sync = true;
    AlibabaCloud::OSS::g_mill = 0;

    AlibabaCloud::OSS::InitializeSdk();


    srand((unsigned int)time(NULL));
    AlibabaCloud::OSS::init_key();
    if (argc < 3) {
        std::cout << argv[0] << "  put|get  " << "  threadnum  " << "  size "<<std::endl;
        return -1;
    }

    int threadnum = atoi(argv[2]);
    int size      = atoi(argv[3]);

    int datasize = (1024 > size)?size:1024;
    int tsize = (TSIZE>threadnum)?threadnum:TSIZE;
    for (int i =0; i <tsize; i ++ ) {
        for (int j = 0; j < datasize; j++) {
            AlibabaCloud::OSS::g_data[i][j] = AlibabaCloud::OSS::GetRandomStream(4096);
        }
    }
    std::cout <<"init finished" <<std::endl;
    perf_run(argv[1], threadnum, size);
    return 0;
}
