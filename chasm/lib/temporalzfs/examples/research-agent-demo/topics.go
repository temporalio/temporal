package main

import "fmt"

// TopicEntry holds a research topic with its display name and URL-safe slug.
type TopicEntry struct {
	Name string
	Slug string
}

// Topics is a curated list of research topics spanning science, technology,
// policy, medicine, and engineering. The demo runner picks from this list
// and wraps with a numeric suffix when more workflows are needed.
var Topics = []TopicEntry{
	// — Computer Science & AI —
	{"Quantum Computing", "quantum-computing"},
	{"Large Language Models", "large-language-models"},
	{"Reinforcement Learning", "reinforcement-learning"},
	{"Federated Learning", "federated-learning"},
	{"Neuromorphic Computing", "neuromorphic-computing"},
	{"Homomorphic Encryption", "homomorphic-encryption"},
	{"Zero-Knowledge Proofs", "zero-knowledge-proofs"},
	{"Autonomous Vehicles", "autonomous-vehicles"},
	{"Computer Vision", "computer-vision"},
	{"Natural Language Processing", "natural-language-processing"},
	{"Robotics and Automation", "robotics-and-automation"},
	{"Edge Computing", "edge-computing"},
	{"Blockchain Consensus Mechanisms", "blockchain-consensus"},
	{"Differential Privacy", "differential-privacy"},
	{"AI Safety and Alignment", "ai-safety"},
	{"Explainable AI", "explainable-ai"},
	{"Generative Adversarial Networks", "generative-adversarial-networks"},
	{"Graph Neural Networks", "graph-neural-networks"},
	{"Swarm Intelligence", "swarm-intelligence"},
	{"Automated Theorem Proving", "automated-theorem-proving"},

	// — Biology & Medicine —
	{"CRISPR Gene Editing", "crispr-gene-editing"},
	{"mRNA Therapeutics", "mrna-therapeutics"},
	{"Synthetic Biology", "synthetic-biology"},
	{"Microbiome Research", "microbiome-research"},
	{"Protein Folding", "protein-folding"},
	{"CAR-T Cell Therapy", "car-t-cell-therapy"},
	{"Epigenetics", "epigenetics"},
	{"Brain-Computer Interfaces", "brain-computer-interfaces"},
	{"Longevity Research", "longevity-research"},
	{"Pandemic Preparedness", "pandemic-preparedness"},
	{"Antibiotic Resistance", "antibiotic-resistance"},
	{"Stem Cell Therapy", "stem-cell-therapy"},
	{"Precision Medicine", "precision-medicine"},
	{"Optogenetics", "optogenetics"},
	{"Gut-Brain Axis", "gut-brain-axis"},
	{"Vaccine Development", "vaccine-development"},
	{"Regenerative Medicine", "regenerative-medicine"},
	{"Immunotherapy", "immunotherapy"},
	{"Bioprinting", "bioprinting"},
	{"Pharmacogenomics", "pharmacogenomics"},

	// — Physics & Space —
	{"Dark Matter Detection", "dark-matter-detection"},
	{"Fusion Energy", "fusion-energy"},
	{"Gravitational Waves", "gravitational-waves"},
	{"Exoplanet Habitability", "exoplanet-habitability"},
	{"Space Debris Mitigation", "space-debris-mitigation"},
	{"Quantum Gravity", "quantum-gravity"},
	{"Neutrino Physics", "neutrino-physics"},
	{"Topological Materials", "topological-materials"},
	{"Superconductivity", "superconductivity"},
	{"Asteroid Mining", "asteroid-mining"},
	{"Mars Colonization", "mars-colonization"},
	{"Solar Sail Propulsion", "solar-sail-propulsion"},
	{"Cosmic Microwave Background", "cosmic-microwave-background"},
	{"Black Hole Information Paradox", "black-hole-information-paradox"},
	{"Plasma Physics", "plasma-physics"},

	// — Energy & Environment —
	{"Climate Change Modeling", "climate-change-modeling"},
	{"Carbon Capture", "carbon-capture"},
	{"Ocean Acidification", "ocean-acidification"},
	{"Solid-State Batteries", "solid-state-batteries"},
	{"Hydrogen Economy", "hydrogen-economy"},
	{"Perovskite Solar Cells", "perovskite-solar-cells"},
	{"Wind Energy Optimization", "wind-energy-optimization"},
	{"Geothermal Energy", "geothermal-energy"},
	{"Biodiversity Loss", "biodiversity-loss"},
	{"Coral Reef Restoration", "coral-reef-restoration"},
	{"Desalination Technology", "desalination-technology"},
	{"Smart Grid Systems", "smart-grid-systems"},
	{"Circular Economy", "circular-economy"},
	{"Arctic Ice Dynamics", "arctic-ice-dynamics"},
	{"Wildfire Prediction", "wildfire-prediction"},

	// — Engineering & Materials —
	{"Metamaterials", "metamaterials"},
	{"Additive Manufacturing", "additive-manufacturing"},
	{"Self-Healing Materials", "self-healing-materials"},
	{"Graphene Applications", "graphene-applications"},
	{"Digital Twins", "digital-twins"},
	{"Soft Robotics", "soft-robotics"},
	{"Autonomous Drones", "autonomous-drones"},
	{"Hyperloop Transport", "hyperloop-transport"},
	{"Vertical Farming", "vertical-farming"},
	{"Lab-Grown Meat", "lab-grown-meat"},
	{"Quantum Sensors", "quantum-sensors"},
	{"Wearable Health Tech", "wearable-health-tech"},
	{"Nuclear Microreactors", "nuclear-microreactors"},
	{"Photonic Computing", "photonic-computing"},
	{"Flexible Electronics", "flexible-electronics"},

	// — Social Sciences & Policy —
	{"Universal Basic Income", "universal-basic-income"},
	{"Digital Currency Policy", "digital-currency-policy"},
	{"Misinformation Detection", "misinformation-detection"},
	{"Algorithmic Fairness", "algorithmic-fairness"},
	{"Cybersecurity Frameworks", "cybersecurity-frameworks"},
	{"Data Sovereignty", "data-sovereignty"},
	{"Post-Quantum Cryptography", "post-quantum-cryptography"},
	{"Smart Cities", "smart-cities"},
	{"Digital Identity Systems", "digital-identity-systems"},
	{"Open Source Intelligence", "open-source-intelligence"},
	{"Supply Chain Resilience", "supply-chain-resilience"},
	{"Telemedicine Adoption", "telemedicine-adoption"},
	{"EdTech and Learning Science", "edtech-learning-science"},
	{"Remote Work Productivity", "remote-work-productivity"},
	{"Autonomous Weapons Policy", "autonomous-weapons-policy"},

	// — Mathematics & Theory —
	{"Topological Data Analysis", "topological-data-analysis"},
	{"Causal Inference", "causal-inference"},
	{"Information Theory", "information-theory"},
	{"Complexity Theory", "complexity-theory"},
	{"Category Theory Applications", "category-theory-applications"},
	{"Bayesian Optimization", "bayesian-optimization"},
	{"Numerical Weather Prediction", "numerical-weather-prediction"},
	{"Network Science", "network-science"},
	{"Chaos Theory Applications", "chaos-theory-applications"},
	{"Computational Geometry", "computational-geometry"},
}

// TopicForIndex returns a topic for the given index, wrapping with a numeric
// suffix when the index exceeds the topic list length.
func TopicForIndex(i int) TopicEntry {
	if i < len(Topics) {
		return Topics[i]
	}
	base := Topics[i%len(Topics)]
	cycle := i/len(Topics) + 1
	return TopicEntry{
		Name: fmt.Sprintf("%s (%d)", base.Name, cycle),
		Slug: fmt.Sprintf("%s-%d", base.Slug, cycle),
	}
}
