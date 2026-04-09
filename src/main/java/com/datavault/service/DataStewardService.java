package com.datavault.service;

import com.datavault.dto.DataStewardDTO;
import com.datavault.entity.DataSteward;
import com.datavault.repository.DataStewardRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DataStewardService {
    
    private final DataStewardRepository dataStewardRepository;

    public List<DataStewardDTO> getAllStewards() {
        return dataStewardRepository.findAll().stream()
            .map(this::mapToDTO)
            .collect(Collectors.toList());
    }
    
    public List<DataStewardDTO> getActiveStewards() {
        return dataStewardRepository.findByIsActive(true).stream()
            .map(this::mapToDTO)
            .collect(Collectors.toList());
    }
    
    public DataStewardDTO getStewardById(Long id) {
        DataSteward steward = dataStewardRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Data steward not found"));
        return mapToDTO(steward);
    }
    
    @Transactional
    public DataStewardDTO createSteward(DataStewardDTO dto) {
        log.info("Creating data steward: {}", dto.getName());
        
        DataSteward steward = DataSteward.builder()
            .name(dto.getName())
            .email(dto.getEmail())
            .department(dto.getDepartment())
            .role(dto.getRole())
            .phoneNumber(dto.getPhoneNumber())
            .isActive(dto.getIsActive() != null ? dto.getIsActive() : true)
            .build();
        
        steward = dataStewardRepository.save(steward);
        return mapToDTO(steward);
    }
    
    @Transactional
    public DataStewardDTO updateSteward(Long id, DataStewardDTO dto) {
        log.info("Updating data steward id: {}", id);
        
        DataSteward steward = dataStewardRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Data steward not found"));
        
        steward.setName(dto.getName());
        steward.setEmail(dto.getEmail());
        steward.setDepartment(dto.getDepartment());
        steward.setRole(dto.getRole());
        steward.setPhoneNumber(dto.getPhoneNumber());
        steward.setIsActive(dto.getIsActive());
        
        steward = dataStewardRepository.save(steward);
        return mapToDTO(steward);
    }
    
    @Transactional
    public void deleteSteward(Long id) {
        log.info("Deleting data steward id: {}", id);
        dataStewardRepository.deleteById(id);
    }
    
    private DataStewardDTO mapToDTO(DataSteward steward) {
        return DataStewardDTO.builder()
            .id(steward.getId())
            .name(steward.getName())
            .email(steward.getEmail())
            .department(steward.getDepartment())
            .role(steward.getRole())
            .phoneNumber(steward.getPhoneNumber())
            .isActive(steward.getIsActive())
            .createdAt(steward.getCreatedAt())
            .build();
    }
}
