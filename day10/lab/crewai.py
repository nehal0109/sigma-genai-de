class LLM:
    def __init__(self, *args, **kwargs):
        pass


class Agent:
    def __init__(self, role, goal, backstory, **kwargs):
        self.role = role
        self.goal = goal
        self.backstory = backstory


class Task:
    def __init__(self, description, expected_output, agent, context=None):
        self.description = description
        self.expected_output = expected_output
        self.agent = agent
        self.context = context or []


class Process:
    sequential = "sequential"


class Crew:
    def __init__(self, agents, tasks, process=None, verbose=False):
        self.agents = agents
        self.tasks = tasks

    def kickoff(self):
        return """Executive summary: Silver layer has manageable quality issues.
-- FIX: duplicate transaction check | SEVERITY: HIGH | RISK: LOW
UPDATE silver_transactions SET quality_flag='REVIEW' WHERE transaction_id IS NOT NULL AND amount >= 0;
-- ROLLBACK: restore quality_flag from backup table
Guardian verdict: SAFE TO RUN with monitoring. Data health score: 8/10."""
