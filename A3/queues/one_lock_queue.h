#include "../common/allocator.h"
#include <pthread.h>

template <class T>
class Node
{
    public:
    T value;
    Node<T>* next;
};

template <class T>
class OneLockQueue
{
    Node<T>* q_head;
    Node<T>* q_tail;
    pthread_mutex_t queueLock;

    CustomAllocator my_allocator_;
public:
    OneLockQueue() : my_allocator_()
    {
        std::cout << "Using OneLockQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next = nullptr;
        q_head = newNode; 
        q_tail = newNode;
        my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
        Node<T> *node = (Node<T>* )my_allocator_.newNode();
        node->value = value;
        node->next = NULL;
        pthread_mutex_lock(&queueLock);
        q_tail->next = node;
        q_tail = node;
        pthread_mutex_unlock(&queueLock);
    }

    bool dequeue(T *value)
    {   
        pthread_mutex_lock(&queueLock);
        Node<T> *node = q_head;
        Node<T> *new_head = q_head->next;
        if(new_head == NULL){
            // Queue is empty
            pthread_mutex_unlock(&queueLock);
            return false;
        }
        *value = new_head->value;
        q_head = new_head;
        pthread_mutex_unlock(&queueLock);

        my_allocator_.freeNode(node);
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};