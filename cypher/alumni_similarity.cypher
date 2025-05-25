MATCH (u1:Person {email: $targetEmail})
WHERE NOT (u1)-[:HAS_SCHOLARSHIP]->()

WITH u1
MATCH (u2:Person)
WHERE u2 <> u1 AND (u2)-[:HAS_SCHOLARSHIP]->(:Scholarship {scholarship_name: $targetScholarship})

// Calcular similaridade
WITH u1, u2,
     // Habilidades em comum
     SIZE([(u1)-[:HAS_SKILL]->(skill)<-[:HAS_SKILL]-(u2) | skill]) AS skillsMatch,
     
     // Idiomas em comum
     SIZE([(u1)-[:SPEAKS]->(lang)<-[:SPEAKS]-(u2) | lang]) AS languagesMatch,
     
     // Educação em comum
     SIZE([(u1)-[:HAS_DEGREE]->(degree)<-[:HAS_DEGREE]-(u2) | degree]) AS educationMatch,
     
     // Certificações em comum
     SIZE([(u1)-[:HAS_CERTIFICATION]->(cert)<-[:HAS_CERTIFICATION]-(u2) | cert]) AS certsMatch,
     
     // Honras em comum
     SIZE([(u1)-[:HAS_HONOR]->(honor)<-[:HAS_HONOR]-(u2) | honor]) AS honorsMatch,
     
     // Experiências em comum
     SIZE([(u1)-[:WORKED_AT]->(company)<-[:WORKED_AT]-(u2) | company]) AS companiesMatch

// Calcular pontuação total
WITH u2, skillsMatch + languagesMatch + educationMatch + certsMatch + honorsMatch + companiesMatch AS similarityScore

ORDER BY similarityScore DESC
LIMIT 5
RETURN u2.email AS SimilarUser, similarityScore AS TotalMatches
