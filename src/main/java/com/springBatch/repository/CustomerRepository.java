package com.springBatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.springBatch.entity.Customer;

public interface CustomerRepository  extends JpaRepository<Customer,Integer> {
}
