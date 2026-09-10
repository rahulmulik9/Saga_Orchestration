package com.rahul.inventoryservice.service;

import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.exception.InsufficientStockException;
import com.rahul.inventoryservice.repository.ProductRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    public Product getProductById(Long id) {
        return productRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Product not found with id: " + id));
    }

    public Product createProduct(Product product) {
        return productRepository.save(product);
    }

    public Product updateProduct(Long id, Product updatedProduct) {
        Product existing = getProductById(id);
        existing.setName(updatedProduct.getName());
        existing.setPrice(updatedProduct.getPrice());
        existing.setQuantity(updatedProduct.getQuantity());
        return productRepository.save(existing);
    }

    @Transactional
    public Product deductStock(Long id, int quantity) {
        Product product = getProductById(id);

        if (product.getQuantity() < quantity) {
            throw new InsufficientStockException(
                    "Insufficient stock for product id " + id
                            + ": requested " + quantity + ", available " + product.getQuantity());
        }

        product.setQuantity(product.getQuantity() - quantity);
        return productRepository.save(product);
    }
}