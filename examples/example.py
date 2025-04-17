"""
A standard FastAPI application.
Run with: uvicorn app:app --reload
"""
from fastapi import FastAPI, Query, Path
from pydantic import BaseModel
from typing import Optional, List

# Create FastAPI app
app = FastAPI(title="Todo API")

# Define data models
class TodoItem(BaseModel):
    id: Optional[int] = None
    content: str
    completed: bool = False

# In-memory database
todos = [
    TodoItem(id=1, content="Learn FastAPI", completed=True),
    TodoItem(id=2, content="Build a REST API", completed=False),
    TodoItem(id=3, content="Deploy to production", completed=False),
]

# API endpoints
@app.get("/")
def read_root():
    return {"message": "Welcome to the Todo API"}

@app.get("/todos", response_model=List[TodoItem])
def list_todos(skip: int = 0, limit: int = 10):
    """Get all todos with pagination."""
    return todos[skip: skip + limit]

@app.get("/todos/{todo_id}", response_model=TodoItem)
def get_todo(todo_id: int = Path(..., description="The ID of the todo to retrieve")):
    """Get a specific todo by ID."""
    for todo in todos:
        if todo.id == todo_id:
            return todo
    raise HTTPException(status_code=404, detail="Todo not found")

@app.post("/todos", response_model=TodoItem, status_code=201)
def create_todo(todo: TodoItem):
    """Create a new todo."""
    # Auto-increment ID
    if todo.id is None:
        todo.id = max(t.id for t in todos) + 1 if todos else 1
    todos.append(todo)
    return todo

@app.put("/todos/{todo_id}", response_model=TodoItem)
def update_todo(todo_id: int, updated_todo: TodoItem):
    """Update a todo by ID."""
    for i, todo in enumerate(todos):
        if todo.id == todo_id:
            updated_todo.id = todo_id  # Ensure ID stays the same
            todos[i] = updated_todo
            return updated_todo
    raise HTTPException(status_code=404, detail="Todo not found")

@app.delete("/todos/{todo_id}", status_code=204)
def delete_todo(todo_id: int):
    """Delete a todo by ID."""
    global todos
    original_length = len(todos)
    todos = [todo for todo in todos if todo.id != todo_id]
    if len(todos) == original_length:
        raise HTTPException(status_code=404, detail="Todo not found")
    return None

# If the file is run directly, start the app with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
