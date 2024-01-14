#include "../common/allocator.h"
#include "../common/utils.h"

#define LFENCE asm volatile("lfence" : : : "memory")
#define SFENCE asm volatile("sfence" : : : "memory")

template <class P>
struct pointer_t{
    P* ptr;
    pointer_t(){
        ptr = nullptr;
    }
    pointer_t(P* val, uintptr_t count){
        ptr = val + (count << 48);
    }
    P* address(){
        return  (P*)((uintptr_t)ptr & 0xFFFFFFFFFFFF );
    }
    uint count(){
        return ((uintptr_t)ptr >> 48);
    }
};

template <class T>
class Node
{
public:
    T value;
    pointer_t<Node<T>> next;
};

template <class T>
class NonBlockingQueue
{
    pointer_t<Node<T>> q_head;
    pointer_t<Node<T>> q_tail;
    CustomAllocator my_allocator_;
public:
    
    NonBlockingQueue() : my_allocator_()
    {
        std::cout << "Using NonBlockingQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next.ptr = nullptr;
        q_head.ptr = newNode; 
        q_tail.ptr = newNode;
        my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
        // Use LFENCE and SFENCE as mentioned in pseudocode
        pointer_t<Node<T>> tail;
        pointer_t<Node<T>> next;
        Node<T> *node = (Node<T>* )my_allocator_.newNode();
        node->value = value;
        node->next.ptr = nullptr;
        SFENCE;

        while (true){
            tail = q_tail;
            LFENCE;
            next = tail.address()->next;
            LFENCE;
            if(tail.ptr == q_tail.ptr){
                if(next.address() == nullptr){
                    if(CAS(&tail.address()->next, next, pointer_t<Node<T>>(node, next.count()+1 )))
                        break;
                }
                else 
                    CAS(&q_tail, tail, pointer_t<Node<T>>(next.address(), tail.count()+1));
            } 

        }
        SFENCE;
        CAS(&q_tail, tail, pointer_t<Node<T>>(node , tail.count()+1) )  ;
        
    }

    bool dequeue(T *value)
    {
        // // Use LFENCE and SFENCE as mentioned in pseudocode
        pointer_t<Node<T>> head;
        pointer_t<Node<T>> tail;
        pointer_t<Node<T>> next;
        while(true){
            head = q_head;
            LFENCE;
            tail = q_tail;
            LFENCE;
            next = head.address()->next;
            LFENCE;
            if(head.ptr == q_head.ptr){
                if(head.address() == tail.address()){
                    if(next.address() == nullptr)
                        return false;
                    CAS(&q_tail, tail, pointer_t<Node<T>>(next.address() , tail.count()+1) );
                }
                else{
                    *value = next.address()->value;
                    if( CAS(&q_head, head, pointer_t<Node<T>>(next.address(), head.count()+1) ))
                        break;
                }
            }
        }
        my_allocator_.freeNode(head.address());
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }

};

