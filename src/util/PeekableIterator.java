package util;

import java.util.Iterator;
import java.util.Scanner;

public interface PeekableIterator extends Iterator
{
    public Object peek();
}