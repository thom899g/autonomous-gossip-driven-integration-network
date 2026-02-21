import logging
from datetime import datetime
from typing import Dict, List, Optional
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Node:
    """Represents an autonomous AI module in the gossip network.

    Attributes:
        id: Unique identifier of the node.
        knowledge_base: Dictionary storing known facts.
        neighbors: List of neighboring nodes' IDs.
        gossip_interval: Time interval between gossip exchanges.
        last_gossip_time: Timestamp of the last gossip transmission.
    """

    def __init__(self, node_id: str, initial_knowledge: Optional[Dict[str, bool]] = None):
        """Initialize a Node with given ID and optional initial knowledge."""
        self.id = node_id
        self.knowledge_base = initial_knowledge.copy() if initial_knowledge else {}
        self.neighbors = []
        self.gossip_interval = 30  # seconds between gossip rounds
        self.last_gossip_time = datetime.now()
        
        # Error handling setup
        self.error_count = 0

    def add_neighbor(self, neighbor_id: str):
        """Add a neighboring node to the node's connection list."""
        if neighbor_id not in self.neighbors:
            self.neighbors.append(neighbor_id)
            logger.info(f"Added neighbor {neighbor_id} to {self.id}")

    def remove_neighbor(self, neighbor_id: str) -> bool:
        """Remove a neighboring node from the node's connection list."""
        try:
            self.neighbors.remove(neighbor_id)
            logger.info(f"Removed neighbor {neighbor_id} from {self.id}")
            return True
        except ValueError:
            logger.warning(f"Neighbor {neighbor_id} not found in {self.id}'s connections")
            return False

    def receive_message(self, message: 'Message'):
        """Process an incoming gossip message."""
        try:
            if message.sender == self.id:
                logger.debug("Ignoring own message")
                return
            
            # Validate message signature (could be enhanced with cryptography)
            if not self._validate_signature(message):
                logger.warning(f"Received invalid message from {message.sender}")
                return

            # Process the received command
            self._process_command(message.content)

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            self.error_count += 1

    def _validate_signature(self, message):
        """Stub method for message validation. To be implemented with cryptographic checks."""
        # TODO: Implement proper signature verification
        return True

    def _process_command(self, command: Dict[str, str]):
        """Execute commands received through gossip messages."""
        action = command.get('action', 'noop')
        target_fact = command.get('fact', None)
        
        if action == 'noop':
            logger.debug("Received no-op command")
            return
        elif action == 'update_knowledge' and target_fact:
            self._update_fact(target_fact)
        else:
            logger.warning(f"Unknown command received: {action}")

    def _update_fact(self, fact: str):
        """Update the node's knowledge base with a new fact."""
        if fact in self.knowledge_base:
            if not self.knowledge_base[fact]:
                logger.info(f"Updating fact {fact} to True")
                self.knowledge_base[fact] = True
        else:
            logger.info(f"Adding new fact {fact}")
            self.knowledge_base[fact] = True

    def send_gossip(self):
        """Send gossip messages to neighboring nodes."""
        try:
            if (datetime.now() - self.last_gossip_time).total_seconds() < self.gossip_interval:
                return
            
            # Prepare the gossip message
            known_facts = {fact: value for fact, value in self.knowledge_base.items()}
            command = {'action': 'update_knowledge', 'facts': known_facts}
            
            # Select random subset of neighbors to send gossip
            if len(self.neighbors) > 0:
                selected_neighbors = random.sample(self.neighbors, min(2, len(self.neighbors)))
                for neighbor in selected_neighbors:
                    message = Message(sender=self.id, recipient=neighbor, content=command)
                    logger.info(f"Sent message to {neighbor}")
                
                self.last_gossip_time = datetime.now()

        except Exception as e:
            logger.error(f"Gossip failed: {str(e)}")
            self.error_count += 1

class KnowledgeBase:
    """Manages the knowledge repository for a node.

    Attributes:
        facts: Dictionary of known facts with boolean values.
        change_log: List tracking changes to the knowledge base.
    """

    def __init__(self):
        """Initialize an empty knowledge base."""
        self.facts = {}
        self.change_log = []

    def add_fact(self, fact: str) -> bool:
        """Add a new fact to the knowledge base.

        Returns:
            True if the fact was added/updated, False otherwise.
        """
        current_time = datetime.now()
        if fact in self.facts:
            if self.facts[fact] == True:
                return False
            logger.info(f"Updating existing fact {fact}")
        else:
            logger.info(f"Adding new fact {fact}")

        self.facts[fact] = True
        self._log_change(f"Added/updated fact: {fact}", current_time)
        return True

    def remove_fact(self, fact: str) -> bool:
        """Remove a fact from the knowledge base.

        Returns:
            True if the fact was removed, False otherwise.
        """
        current_time = datetime.now()
        if fact in self.facts and self.facts[fact]:
            del self.facts[fact]
            logger.info(f"Removed fact {fact}")
            self._log_change(f"Removed fact: {fact}", current_time)
            return True
        return False

    def has_fact(self, fact: str) -> bool:
        """Check if a fact is known.

        Returns:
            True if the fact exists and is true, False otherwise.
        """
        return self.facts.get(fact, False)

    def _log_change(self, description: str, timestamp: datetime):
        """Log changes to the knowledge base for auditing."""
        self.change_log.append((timestamp, description))

class Message:
    """Represents a message within the gossip network.

    Attributes:
        sender: ID of the sending node.
        recipient: ID of the receiving node.