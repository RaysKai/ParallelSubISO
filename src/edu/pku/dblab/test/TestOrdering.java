package edu.pku.dblab.test;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

public class TestOrdering {
    public static void main(String[] args) {
        Ordering<String> natural = Ordering.natural();
        
        List<String> abc = ImmutableList.of("a", "b", "c");
        System.out.println(natural.isOrdered(abc));
        
        List<String> cab = ImmutableList.of("c", "a", "b");
        System.out.println(natural.isOrdered(cab));
        System.out.println(cab = natural.sortedCopy(cab));
        System.out.println(natural.max(cab));
        System.out.println(natural.min(cab));
        System.out.println(natural.leastOf(cab, 2));
        System.out.println(natural.greatestOf(cab, 2));
        
        System.out.println("====================================");
        
        Ordering<Integer> intNatural = Ordering.natural();
        
        List<Integer> a123 = ImmutableList.of(1, 2, 3);
        System.out.println(intNatural.isOrdered(a123));
        
        List<Integer> a132 = ImmutableList.of(1, 3, 2);
        System.out.println(intNatural.isOrdered(a132));
        System.out.println(intNatural.sortedCopy(a132));
        
        List<Integer> a321 = ImmutableList.of(3, 2, 1);
        System.out.println(intNatural.reverse().isOrdered(a321));
        
    }
}