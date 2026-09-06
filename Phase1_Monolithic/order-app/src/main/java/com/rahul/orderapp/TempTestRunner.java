package com.rahul.orderapp;

import com.rahul.orderapp.entity.Product;
import com.rahul.orderapp.repository.ProductRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

@Configuration
public class TempTestRunner {

    @Bean
    CommandLineRunner testProductRepo(ProductRepository productRepository) {
        return args -> {
            Product product = new Product();
            product.setName("Test Widget");
            product.setPrice(BigDecimal.valueOf(9.99));
            product.setQuantity(10);

            Product saved = productRepository.save(product);
            System.out.println("Saved product with id: " + saved.getId());

            productRepository.findById(saved.getId())
                    .ifPresent(p -> System.out.println("Fetched back: " + p.getName() + ", qty=" + p.getQuantity()));
        };
    }
}