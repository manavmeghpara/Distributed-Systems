#include "../common/allocator.h"

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
class LockFreeStack
{
    pointer_t<Node<T>> s_top;
    CustomAllocator my_allocator_;
public:
    LockFreeStack() : my_allocator_()
    {
        std::cout << "Using LockFreeStack\n";
    }

    void initStack(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Perform any necessary initializations
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next.ptr = nullptr;
        s_top.ptr = newNode; 
    }

    /**
     * Create a new node with value `value` and update it to be the top of the stack.
     * This operation must always succeed.
     */
    void push(T value)
    {
        Node<T> *node = (Node<T>* )my_allocator_.newNode();

        while (true){
            pointer_t<Node<T>> old_top = s_top;
            LFENCE;
            node->next.ptr = old_top.ptr;
            node->value = value;
            SFENCE;
            if(old_top.ptr == s_top.ptr){
                if (CAS(&s_top.ptr, old_top.ptr, pointer_t<Node<T>>(node, old_top.count() +1).ptr))
                    return;
            }
        }  
    }

    /**
     * If the stack is empty: return false.
     * Otherwise: copy the value at the top of the stack into `value`, update
     * the top to point to the next element in the stack, and return true.
     */
    bool pop(T *value)
    {

        while(true){
            pointer_t<Node<T>> old_top = s_top;
            LFENCE;
            pointer_t<Node<T>> new_top = old_top.address()->next;
            LFENCE;
            if(old_top.ptr == s_top.ptr){
                if( new_top.ptr == nullptr)
                    return false;
                if(CAS(&s_top.ptr, old_top.ptr, pointer_t<Node<T>>(new_top.address(), old_top.count() +1).ptr)){
                    *value = old_top.address()->value;
                    my_allocator_.freeNode(old_top.address());
                    return true;
                }
            }
        }

    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
