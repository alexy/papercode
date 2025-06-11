#!/usr/bin/env python3

import yaml
import os
import logging
from pathlib import Path
from typing import Dict, Optional, Any

# Configure logging
logger = logging.getLogger(__name__)

class Neo4jConfig:
    """Configuration parser for Neo4j connection settings"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize config parser with path to config file"""
        self.config_path = Path(config_path)
        self._config_data = None
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            if not self.config_path.exists():
                logger.warning(f"Config file not found: {self.config_path}")
                logger.info("You can create a config.yaml file to store Neo4j credentials")
                self._config_data = {}
                return
            
            with open(self.config_path, 'r') as f:
                self._config_data = yaml.safe_load(f) or {}
                logger.info(f"✅ Loaded config from: {self.config_path}")
                
                # Log available environments (without passwords)
                environments = list(self._config_data.keys())
                if environments:
                    logger.info(f"📋 Available environments: {', '.join(environments)}")
                
        except yaml.YAMLError as e:
            logger.error(f"❌ Invalid YAML in config file: {e}")
            self._config_data = {}
        except Exception as e:
            logger.error(f"❌ Failed to load config file: {e}")
            self._config_data = {}
    
    def get_environment_config(self, environment: str) -> Optional[Dict[str, str]]:
        """Get configuration for a specific environment"""
        if not self._config_data:
            return None
        
        env_config = self._config_data.get(environment)
        if not env_config:
            available = list(self._config_data.keys())
            logger.error(f"❌ Environment '{environment}' not found in config")
            logger.error(f"Available environments: {available}")
            return None
        
        # Validate required fields
        required_fields = ['uri', 'user', 'password']
        missing_fields = [field for field in required_fields if not env_config.get(field)]
        
        if missing_fields:
            logger.error(f"❌ Missing required fields in '{environment}' config: {missing_fields}")
            return None
        
        # Log connection info (without password)
        logger.info(f"📍 Using {environment} environment:")
        logger.info(f"   URI: {env_config['uri']}")
        logger.info(f"   User: {env_config['user']}")
        if env_config.get('description'):
            logger.info(f"   Description: {env_config['description']}")
        
        return {
            'uri': env_config['uri'],
            'user': env_config['user'],
            'password': env_config['password']
        }
    
    def get_local_config(self) -> Optional[Dict[str, str]]:
        """Get local environment configuration"""
        return self.get_environment_config('local')
    
    def get_remote_config(self) -> Optional[Dict[str, str]]:
        """Get remote environment configuration"""
        return self.get_environment_config('remote')
    
    def list_environments(self) -> list:
        """List all available environments"""
        if not self._config_data:
            return []
        return list(self._config_data.keys())
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate the entire configuration and return status"""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'environments': {}
        }
        
        if not self._config_data:
            validation_result['valid'] = False
            validation_result['errors'].append("No configuration data loaded")
            return validation_result
        
        required_fields = ['uri', 'user', 'password']
        
        for env_name, env_config in self._config_data.items():
            env_status = {'valid': True, 'issues': []}
            
            if not isinstance(env_config, dict):
                env_status['valid'] = False
                env_status['issues'].append("Configuration must be a dictionary")
                validation_result['valid'] = False
                validation_result['errors'].append(f"Invalid config format for environment '{env_name}'")
            else:
                # Check required fields
                for field in required_fields:
                    if not env_config.get(field):
                        env_status['valid'] = False
                        env_status['issues'].append(f"Missing required field: {field}")
                        validation_result['valid'] = False
                        validation_result['errors'].append(f"Environment '{env_name}' missing field: {field}")
                
                # Check for default/insecure passwords
                password = env_config.get('password', '')
                if password in ['password', 'neo4j', '123456', 'admin']:
                    env_status['issues'].append("Using default/insecure password")
                    validation_result['warnings'].append(f"Environment '{env_name}' uses insecure password")
                
                # Check URI format
                uri = env_config.get('uri', '')
                if uri and not (uri.startswith('bolt://') or uri.startswith('neo4j://')):
                    env_status['issues'].append("URI should start with 'bolt://' or 'neo4j://'")
                    validation_result['warnings'].append(f"Environment '{env_name}' URI format may be incorrect")
            
            validation_result['environments'][env_name] = env_status
        
        return validation_result

def create_example_config(file_path: str = "config.yaml"):
    """Create an example configuration file"""
    example_config = {
        'local': {
            'uri': 'bolt://localhost:7687',
            'user': 'neo4j',
            'password': 'password',
            'description': 'Local development Neo4j instance'
        },
        'remote': {
            'uri': 'bolt://remote-server:7687',
            'user': 'neo4j',
            'password': 'remote_password',
            'description': 'Remote production Neo4j instance'
        }
    }
    
    config_path = Path(file_path)
    if config_path.exists():
        logger.warning(f"Config file already exists: {file_path}")
        return False
    
    try:
        with open(config_path, 'w') as f:
            yaml.dump(example_config, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"✅ Created example config file: {file_path}")
        logger.info("📝 Please edit the file with your actual Neo4j credentials")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create config file: {e}")
        return False

if __name__ == "__main__":
    # Test the config parser
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Neo4j configuration parser")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--create-example", action="store_true", help="Create example config file")
    parser.add_argument("--validate", action="store_true", help="Validate config file")
    parser.add_argument("--environment", help="Test specific environment")
    
    args = parser.parse_args()
    
    # Configure logging for testing
    logging.basicConfig(level=logging.INFO)
    
    if args.create_example:
        create_example_config(args.config)
    else:
        config = Neo4jConfig(args.config)
        
        if args.validate:
            validation = config.validate_config()
            print(f"\nValidation Result: {'✅ VALID' if validation['valid'] else '❌ INVALID'}")
            
            if validation['errors']:
                print(f"\nErrors:")
                for error in validation['errors']:
                    print(f"  ❌ {error}")
            
            if validation['warnings']:
                print(f"\nWarnings:")
                for warning in validation['warnings']:
                    print(f"  ⚠️  {warning}")
            
            print(f"\nEnvironments:")
            for env_name, env_status in validation['environments'].items():
                status = "✅ VALID" if env_status['valid'] else "❌ INVALID"
                print(f"  {env_name}: {status}")
                for issue in env_status['issues']:
                    print(f"    - {issue}")
        
        if args.environment:
            env_config = config.get_environment_config(args.environment)
            if env_config:
                print(f"\n{args.environment} environment configuration:")
                print(f"  URI: {env_config['uri']}")
                print(f"  User: {env_config['user']}")
                print(f"  Password: {'*' * len(env_config['password'])}")
            else:
                print(f"Environment '{args.environment}' not found or invalid")
        else:
            environments = config.list_environments()
            if environments:
                print(f"\nAvailable environments: {', '.join(environments)}")
            else:
                print("\nNo environments found in config")