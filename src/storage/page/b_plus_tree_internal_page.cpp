//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(PageType::INTERNAL_PAGE);
    
  page_id_ = page_id;
  
  parent_page_id_ = parent_id;
  
  max_size_ = max_size;
  
  size_ = 0;
  
  // 내부 페이지의 기본적으로 더미 키를 설정 (필요 시)
  array_[0] = MappingType();
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{};
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {     
  for (int i = 0; i < size_; ++i) {
    if (array_[i].second == value) {
        return i;  // 찾은 인덱스를 반환
    }
}

return -1;  // 값이 없으면 -1 반환
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 1; i < size_; i++) {
    // 주어진 키와 비교하여 검색
    if (comparator(key, array_[i].first) < 0) {
        return array_[i].second; 
    }
}
return array_[size_ - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
                                                      SetKeyAt(0, new_key);  // 새로운 키를 첫 번째 위치에 설정
                                                      array_[0].second = new_value;  // 새 자식 페이지 ID 설정
                                                      size_ = 1;
                                                     }
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
                                                      int index = ValueIndex(old_value);
    
                                                      // 새 키-값을 삽입
                                                      for (int i = size_; i > index; --i) {
                                                          array_[i] = array_[i - 1];
                                                      }
                                                      
                                                      array_[index + 1] = MappingType(new_key, new_value);
                                                      size_++;
                                                      
                                                      return size_;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
                                                  int half_size = size_ / 2;
    
                                                  // 수신 페이지에 절반의 데이터를 복사
                                                  for (int i = half_size; i < size_; i++) {
                                                      recipient->CopyFirstFrom(array_[i], buffer_pool_manager);
                                                  }
                                                  
                                                  // 수신 페이지의 부모 ID 업데이트
                                                  recipient->SetParentPageId(parent_page_id_);
                                                  
                                                  // 현재 페이지에서 데이터를 삭제
                                                  size_ = half_size;
                                                }

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; ++i) {
    array_[i] = items[i];
    // 자식 페이지의 부모 ID를 업데이트
    page_id_t child_page_id = array_[i].second;
    BPlusTreePage *child_page = buffer_pool_manager->FetchPage(child_page_id);
    child_page->SetParentPageId(page_id_);
    buffer_pool_manager->UnpinPage(child_page_id, true);
}
size_ = size;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < size_ - 1; ++i) {
    array_[i] = array_[i + 1];
}
size_--;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType only_child = array_[0].second;
  size_ = 0;
  return only_child;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
                                                for (int i = 0; i < size_; i++) {
                                                  recipient->CopyFirstFrom(array_[i], buffer_pool_manager);
                                              }
                                              
                                              // 수신 페이지의 부모 ID 업데이트
                                              recipient->SetParentPageId(parent_page_id_);
                                              
                                              // 현재 페이지 크기 초기화
                                              size_ = 0;
                                               }

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
                                                        recipient->CopyFirstFrom(array_[0], buffer_pool_manager);
    
                                                        // 첫 번째 키-값을 삭제하여 페이지 재구성
                                                        Remove(0);
                                                        
                                                        // 중간 키는 부모 페이지로 전달되어야 하므로 업데이트
                                                        recipient->SetKeyAt(0, middle_key);
                                                      }

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[size_] = pair;
  size_++;
  
  // 자식 페이지의 부모 ID를 업데이트
  page_id_t child_page_id = pair.second;
  BPlusTreePage *child_page = buffer_pool_manager->FetchPage(child_page_id);
  child_page->SetParentPageId(page_id_);
  buffer_pool_manager->UnpinPage(child_page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
                                                        recipient->CopyFirstFrom(array_[size_ - 1], buffer_pool_manager);
    
                                                        // 마지막 키-값 삭제
                                                        size_--;
                                                        
                                                        // 중간 키는 부모 페이지로 전달되어야 하므로 업데이트
                                                        recipient->SetKeyAt(0, middle_key);
                                                       }

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[size_] = pair;
  size_++;
  
  // 자식 페이지의 부모 ID를 업데이트
  page_id_t child_page_id = pair.second;
  BPlusTreePage *child_page = buffer_pool_manager->FetchPage(child_page_id);
  child_page->SetParentPageId(page_id_);
  buffer_pool_manager->UnpinPage(child_page_id, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
