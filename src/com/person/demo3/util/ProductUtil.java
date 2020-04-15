package com.person.demo3.util;

import com.person.demo3.pojo.Product;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//读取txt文件中的数据，通过调用file2list（） 返回product类型的list集合。

public class ProductUtil {

//    public static void main(String[] args) throws IOException {
//
//        String fileName = "140k_products.txt";
//        List<Product> products = file2list(fileName);
//        System.out.println(products.size());
//
//    }

    //将该文档存放进product类型的集合。
    public static List<Product> file2list(String fileName) throws IOException {
        File file = new File(fileName);
        //获取文档里面的内容。
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        ArrayList<Product> products = new ArrayList<>();
        for (String line : lines) {
            Product product = addProduct(line);
            products.add(product);
        }
        return products;

    }


    //将string、类型的内容转换为 product 类型。
    public static Product addProduct(String line) {
        Product product = new Product();
        //每一条数据都是通过 "," 隔开。
        String[] split = line.split(",");
        //设置id ,name ,category,place,price,code
        product.setId(Integer.parseInt(split[0]));
        product.setName(split[1]);
        product.setCategory(split[2]);
        product.setPrice(Float.parseFloat(split[3]));
        product.setPlace(split[4]);
        product.setCode(split[5]);
        return product;
    }
}
