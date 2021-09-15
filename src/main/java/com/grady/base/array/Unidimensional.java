package com.grady.base.array;

/*The unidimensional trait:
 *   1.final length
 *   2.在内存中开辟连续空间
 *   3.存取元素速度快，索引*/

public class Unidimensional {
    public static void main(String[] args) {
        // m1 静态初始化 数据类型[] 数组名 = {element1...}
        int[] arrInt = {1, 2, 3, 4};

        // m2 静态初始化 数据类型[] 数组名 = new 数据类型[]{element1...}
        int[] arrIntM2 = new int[]{1, 2, 3, 4};

        // 赋值
        arrInt[0] = 5;

        // 取值
        int iArr = arrInt[0];

        // 遍历m1
        for (int i = 0; i < arrInt.length; i++) {
            System.out.println(arrInt[i]);
        }
        // 便利m2
        for (int i : arrInt) {
            System.out.println(arrInt[i]);
        }
        // while
        int iWhile = 0;
        while (iWhile < arrInt.length){
            System.out.println(arrInt[iWhile]);
            iWhile++;
        }


    }
}
