/**
 * Copyright lizhaolong(https://github.com/Super-long)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Code comment are all encoded in UTF-8.*/

#include "redis.h"
#include<bits/stdc++.h>

const int ThreadNumber = 100;

void RedisTest(Redis* r, int clientID){
    
    for (int i = 0; i < 5; i++){
        std::string ClientString = std::move(std::to_string(clientID));

        std::string temp = "x " + ClientString + " " + std::to_string(i) + " y";
        r->set(ClientString.c_str(), temp.c_str());

        std::cout << r->get(ClientString.c_str()) << std::endl; 
    }
}

int main(){
    std::vector<std::thread> Threads(ThreadNumber);
    std::vector<Redis*> Items(ThreadNumber);

    for(auto& x : Items){
        x = new Redis();
        if(!x->connect("127.0.0.1", 6379)){
            std::cout << "connect error!\n";
            return 0;
        }
    }

    std::cout << "start testing.\n";

    auto start = std::chrono::high_resolution_clock::now(); 

    for (int i = 0; i < ThreadNumber; i++){
        Threads[i] = std::thread(RedisTest, Items[i], i);
    }
    
    std::for_each(Threads.begin(), Threads.end(), std::mem_fn(&std::thread::join));

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::ratio<1,1000>> time_span 
    = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1,1000>>>(end - start);

    std::cout << time_span.count() << std::endl;

    for (int i = 0; i < ThreadNumber; i++){
        delete Items[i];
    }
    
    return 0;
}