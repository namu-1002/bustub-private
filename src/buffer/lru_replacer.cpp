//===----------------------------------------------------------------------===//                                        
//                                                                                                                      
//                         BusTub                                                                                      
//                                                                                                                      
// lru_replacer.cpp                                                                                                    
//                                                                                                                      
// Identification: src/buffer/lru_replacer.cpp                                                                          
//                                                                                                                      
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group                                                  
//                                                                                                                      
//===----------------------------------------------------------------------===//                                        

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    lru_mutex.lock();

    if (unpinned_frames.empty()) {
         lru_mutex.unlock();
         return false;
    }
    *frame_id = unpinned_frames.front();
    unpinned_frames.pop_front();

    lru_mutex.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    lru_mutex.lock();
    for (auto it = unpinned_frames.begin(); it !=  unpinned_frames.end(); it++) {
        if (*it == frame_id) {
            unpinned_frames.erase(it);
            break;
        }
    }
    lru_mutex.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    lru_mutex.lock();
    for (auto it = unpinned_frames.begin(); it != unpinned_frames.end(); ++it) {
        if (*it == frame_id) {
            lru_mutex.unlock();
            return;
        }
    }
    unpinned_frames.push_back(frame_id);
    lru_mutex.unlock();
}

size_t LRUReplacer::Size() {
    lru_mutex.lock();
    size_t size = unpinned_frames.size();
    lru_mutex.unlock();
    return size;
}

}  // namespace bustub
