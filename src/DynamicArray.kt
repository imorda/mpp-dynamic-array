package mpp.dynamicarray

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls

/**
 * @author Belousov Timofey
 */

interface DynamicArray<E> {
    /**
     * Returns the element located in the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun get(index: Int): E

    /**
     * Puts the specified [element] into the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun put(index: Int, element: E)

    /**
     * Adds the specified [element] to this array
     * increasing its [size].
     */
    fun pushBack(element: E)

    /**
     * Returns the current size of this array,
     * it increases with [pushBack] invocations.
     */
    val size: Int
}

interface Descriptor {
    fun complete()
}

class DynamicArrayImpl<E> : DynamicArray<E> {
    private val core = atomic(Core<E>(INITIAL_CAPACITY))

    override fun get(index: Int): E = core.value.get(index)

    override fun put(index: Int, element: E) = core.value.put(index, element)

    override fun pushBack(element: E) {
        val result = core.value.pushBack(element)
        while (true) {
            val curCore = core.value
            if (curCore.capacity < result.capacity) {
                if (!core.compareAndSet(curCore, result)) continue
            }
            break
        }
    }

    override val size: Int get() = core.value.size
}

private class Core<E>(
    capacity: Int,
) {
    val array = atomicArrayOfNulls<Any?>(capacity)
    private val _size = atomic(0)

    val size: Int
        get() = _size.value

    val next: AtomicRef<Core<E>?> = atomic(null)

    val capacity: Int
        get() = array.size

    /**
    @return false if no resizing took place, true otherwise
     */
    @Suppress("UNCHECKED_CAST")
    fun pushBack(element: E): Core<E> {
        while (true) {
            val newPos = size
            if (newPos < array.size) {
                if (array[newPos].compareAndSet(null, element)) {
                    _size.compareAndSet(newPos, newPos + 1)
                    return this
                } else {
                    _size.compareAndSet(newPos, newPos + 1)
                    continue
                }
            } else {
                //The *hard* way
                val newCore = getNextCore()

                for (i in 0..<newPos) {
                    while (true) {
                        val curVal = array[i].value
                        if (curVal is Descriptor) {
                            curVal.complete()
                            break
                        }
                        if (curVal is Stolen) {
                            break
                        }

                        val copyDescriptor = FrozenDescriptor(this, curVal as E, newCore, i)

                        if (!array[i].compareAndSet(curVal, copyDescriptor)) {
                            continue
                        }
                        copyDescriptor.complete()
                        break
                    }
                }

                newCore._size.compareAndSet(0, newPos)

                return newCore.pushBack(element)
            }
        }
    }

    fun get(index: Int): E {
        require(index < size)

        return _get(index)
    }

    @Suppress("UNCHECKED_CAST")
    private fun _get(index: Int): E {
        while (true) {
            val curVal = array[index].value
            if (curVal is Descriptor) {
                curVal.complete()
                continue
            }
            if (curVal is Stolen) {
                return getNextCore()._get(index)
            }

            return curVal as E
        }
    }

    fun put(index: Int, element: E) {
        require(index < size)
        _put(index, element)
    }

    private fun _put(index: Int, element: E) {
        while (true) {
            val curVal = array[index].value
            if (curVal is Descriptor) {
                curVal.complete()
                continue
            }
            if (curVal is Stolen) {
                return getNextCore()._put(index, element)
            }
            if (!array[index].compareAndSet(curVal, element)) {
                continue
            }

            return
        }
    }

    private fun getNextCore(): Core<E> {
        val curNext = next.value
        if (curNext != null) return curNext

        val newNext = Core<E>(array.size * 2)

        return if (next.compareAndSet(null, newNext)) {
            newNext
        } else {
            next.value!!
        }
    }
}

private object Stolen

private class FrozenDescriptor<E>(val oldCore: Core<E>, val value: E, val array: Core<E>, val pos: Int) :
    Descriptor {
    override fun complete() {
        array.array[pos].compareAndSet(null, value)
        oldCore.array[pos].compareAndSet(this, Stolen)
    }

}


private const val INITIAL_CAPACITY = 1 // DO NOT CHANGE ME