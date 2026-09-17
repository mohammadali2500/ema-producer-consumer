package com.example.fx.consumer.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FxQuoteRepository extends JpaRepository<FxQuoteEntity, String> {}
