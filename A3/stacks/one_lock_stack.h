#include "../common/allocator.h"

template <class T>
class Node
{
    public:
    T value;
    Node<T>* next;
};

template <class T>
class OneLockStack
{
    Node<T>* s_top;
    pthread_mutex_t stackLock;
    CustomAllocator my_allocator_;
public:
    OneLockStack() : my_allocator_()
    {
        std::cout << "Using OneLockStack\n";
    }

    void initStack(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Perform any necessary initializations
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next = nullptr;
        s_top = newNode; 
    }

    /**
     * Create a new node with value `value` and update it to be the top of the stack.
     * This operation must always succeed.
     */
    void push(T value)
    {
        Node<T> *node = (Node<T>* )my_allocator_.newNode();
        node->value = value;
        pthread_mutex_lock(&stackLock);
        node->next = s_top->next;
        s_top->next = node;
        pthread_mutex_unlock(&stackLock);   
    }

    /**
     * If the stack is empty: return false.
     * Otherwise: copy the value at the top of the stack into `value`, update
     * the top to point to the next element in the stack, and return true.
     */
    bool pop(T *value)
    {
        pthread_mutex_lock(&stackLock);
        Node<T> *top = s_top;
        Node<T> *old_next = s_top->next;
        if(old_next == nullptr){
            // Queue is empty
            pthread_mutex_unlock(&stackLock);
            return false;
        }
        *value = old_next->value;
        s_top->next = old_next->next;
        pthread_mutex_unlock(&stackLock);

        my_allocator_.freeNode(old_next);
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
