package com.datavault.controller;

import com.datavault.service.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequiredArgsConstructor
public class ViewController {
    
    private final DatabaseService databaseService;
    private final ChangeTrackingService changeTrackingService;

    @GetMapping("/")
    public String index(Model model) {
        String username = "admin"; // Default admin user
        
        // Get statistics
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalDatabases", 12);
        stats.put("totalTables", 487);
        stats.put("piiFields", 245);
        
        model.addAttribute("stats", stats);
        model.addAttribute("databases", databaseService.getAllDatabases(username));
        model.addAttribute("recentChanges", changeTrackingService.getRecentChanges(10));
        
        return "index";
    }

    @GetMapping("/dashboard")
    public String dashboard(Model model) {
        return "dashboard";
    }
    
    @GetMapping("/catalog")
    public String catalog(Model model) {
        String username = "admin";
        model.addAttribute("databases", databaseService.getAllDatabases(username));
        return "catalog";
    }

    @GetMapping("/lineage")
    public String lineage(Model model) {
        return "lineage";
    }

    @GetMapping("/glossary")
    public String glossary(Model model) {
        return "glossary";
    }

    @GetMapping("/governance")
    public String governance(Model model) {
        return "governance";
    }

    @GetMapping("/quality")
    public String quality(Model model) {
        return "quality";
    }

    @GetMapping("/databases/{id}")
    public String databaseDetail(@PathVariable Long id, Model model) {
        model.addAttribute("database", databaseService.getDatabaseById(id));
        return "database-detail";
    }
    
    @GetMapping("/search")
    public String search(@RequestParam String query, Model model) {
        model.addAttribute("query", query);
        return "search";
    }
    
    @GetMapping("/databases/new")
    public String newDatabase(Model model) {
        return "database-connect";
    }
    
    @GetMapping("/glossary/new")
    public String newGlossaryTerm(Model model) {
        return "glossary-form";
    }
    
    @GetMapping("/glossary/edit/{id}")
    public String editGlossaryTerm(@PathVariable Long id, Model model) {
        // Would load term from service
        return "glossary-form";
    }
}
