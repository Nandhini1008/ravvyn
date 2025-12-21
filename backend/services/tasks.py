"""
Tasks Service - Task Management like Google Tasks
Handles CRUD operations for tasks with deadline management
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc
import logging

from services.database import Task, get_db_context
from core.exceptions import DatabaseError, ValidationError, NotFoundError, ServiceError

logger = logging.getLogger(__name__)


class TasksService:
    """Service for managing tasks"""
    
    def create_task(
        self,
        user_id: str,
        title: str,
        description: Optional[str] = None,
        due_date: Optional[datetime] = None,
        priority: str = 'medium',
        db: Optional[Session] = None
    ) -> Dict:
        """
        Create a new task
        
        Args:
            user_id: User identifier
            title: Task title
            description: Task description (optional)
            due_date: Due date (optional)
            priority: Priority level (low, medium, high, urgent)
            db: Optional database session
        
        Returns:
            Dictionary with task information
        
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If database operation fails
        """
        if not title or not isinstance(title, str) or not title.strip():
            raise ValidationError("title is required and must be a non-empty string", field="title")
        
        if len(title) > 500:
            raise ValidationError("title must be 500 characters or less", field="title")
        
        valid_priorities = ['low', 'medium', 'high', 'urgent']
        if priority not in valid_priorities:
            raise ValidationError(f"priority must be one of {valid_priorities}", field="priority")
        
        if db is not None:
            return self._create_task_impl(user_id, title, description, due_date, priority, db)
        else:
            with get_db_context() as db:
                return self._create_task_impl(user_id, title, description, due_date, priority, db)
    
    def _create_task_impl(
        self,
        user_id: str,
        title: str,
        description: Optional[str],
        due_date: Optional[datetime],
        priority: str,
        db: Session
    ) -> Dict:
        """Internal implementation"""
        try:
            # Determine initial status based on due date
            status = 'pending'
            if due_date and due_date < datetime.utcnow():
                status = 'overdue'
            
            task = Task(
                user_id=user_id,
                title=title.strip(),
                description=description.strip() if description else None,
                due_date=due_date,
                priority=priority,
                status=status
            )
            
            db.add(task)
            db.flush()
            
            result = {
                'id': task.id,
                'user_id': task.user_id,
                'title': task.title,
                'description': task.description,
                'status': task.status,
                'priority': task.priority,
                'due_date': task.due_date.isoformat() if task.due_date else None,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None
            }
            
            logger.info(f"Successfully created task {task.id} for user {user_id}")
            return result
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            raise DatabaseError(
                f"Failed to create task: {str(e)}",
                operation="create_task"
            )
    
    def get_task(self, task_id: int, user_id: str, db: Optional[Session] = None) -> Dict:
        """
        Get a specific task
        
        Args:
            task_id: Task ID
            user_id: User identifier
            db: Optional database session
        
        Returns:
            Dictionary with task information
        
        Raises:
            NotFoundError: If task not found
        """
        if db is not None:
            return self._get_task_impl(task_id, user_id, db)
        else:
            with get_db_context() as db:
                return self._get_task_impl(task_id, user_id, db)
    
    def _get_task_impl(self, task_id: int, user_id: str, db: Session) -> Dict:
        """Internal implementation"""
        try:
            task = db.query(Task).filter(
                and_(Task.id == task_id, Task.user_id == user_id)
            ).first()
            
            if not task:
                raise NotFoundError("task", str(task_id))
            
            return {
                'id': task.id,
                'user_id': task.user_id,
                'title': task.title,
                'description': task.description,
                'status': task.status,
                'priority': task.priority,
                'due_date': task.due_date.isoformat() if task.due_date else None,
                'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None
            }
        except NotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error getting task: {str(e)}")
            raise DatabaseError(
                f"Failed to get task: {str(e)}",
                operation="get_task"
            )
    
    def list_tasks(
        self,
        user_id: str,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        include_overdue: bool = True,
        db: Optional[Session] = None
    ) -> List[Dict]:
        """
        List tasks for a user
        
        Args:
            user_id: User identifier
            status: Filter by status (optional)
            priority: Filter by priority (optional)
            include_overdue: Include overdue tasks
            db: Optional database session
        
        Returns:
            List of task dictionaries
        """
        if db is not None:
            return self._list_tasks_impl(user_id, status, priority, include_overdue, db)
        else:
            with get_db_context() as db:
                return self._list_tasks_impl(user_id, status, priority, include_overdue, db)
    
    def _list_tasks_impl(
        self,
        user_id: str,
        status: Optional[str],
        priority: Optional[str],
        include_overdue: bool,
        db: Session
    ) -> List[Dict]:
        """Internal implementation"""
        try:
            query = db.query(Task).filter(Task.user_id == user_id)
            
            # Filter by status
            if status:
                if status == 'active':
                    # Active = pending or in_progress
                    query = query.filter(Task.status.in_(['pending', 'in_progress']))
                else:
                    query = query.filter(Task.status == status)
            elif not include_overdue:
                query = query.filter(Task.status != 'overdue')
            
            # Filter by priority
            if priority:
                query = query.filter(Task.priority == priority)
            
            # Order by due date (soonest first), then by priority
            tasks = query.order_by(
                Task.due_date.asc().nullslast(),
                desc(Task.priority == 'urgent'),
                desc(Task.priority == 'high'),
                Task.created_at.desc()
            ).all()
            
            # Update overdue status
            now = datetime.utcnow()
            result = []
            for task in tasks:
                # Check if task is overdue
                if task.due_date and task.due_date < now and task.status not in ['completed', 'cancelled']:
                    if task.status != 'overdue':
                        task.status = 'overdue'
                        db.flush()
                
                result.append({
                    'id': task.id,
                    'user_id': task.user_id,
                    'title': task.title,
                    'description': task.description,
                    'status': task.status,
                    'priority': task.priority,
                    'due_date': task.due_date.isoformat() if task.due_date else None,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                    'created_at': task.created_at.isoformat() if task.created_at else None,
                    'updated_at': task.updated_at.isoformat() if task.updated_at else None
                })
            
            return result
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            raise DatabaseError(
                f"Failed to list tasks: {str(e)}",
                operation="list_tasks"
            )
    
    def update_task(
        self,
        task_id: int,
        user_id: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        due_date: Optional[datetime] = None,
        db: Optional[Session] = None
    ) -> Dict:
        """
        Update a task
        
        Args:
            task_id: Task ID
            user_id: User identifier
            title: New title (optional)
            description: New description (optional)
            status: New status (optional)
            priority: New priority (optional)
            due_date: New due date (optional)
            db: Optional database session
        
        Returns:
            Updated task dictionary
        
        Raises:
            NotFoundError: If task not found
            ValidationError: If inputs are invalid
        """
        if db is not None:
            return self._update_task_impl(task_id, user_id, title, description, status, priority, due_date, db)
        else:
            with get_db_context() as db:
                return self._update_task_impl(task_id, user_id, title, description, status, priority, due_date, db)
    
    def _update_task_impl(
        self,
        task_id: int,
        user_id: str,
        title: Optional[str],
        description: Optional[str],
        status: Optional[str],
        priority: Optional[str],
        due_date: Optional[datetime],
        db: Session
    ) -> Dict:
        """Internal implementation"""
        try:
            task = db.query(Task).filter(
                and_(Task.id == task_id, Task.user_id == user_id)
            ).first()
            
            if not task:
                raise NotFoundError("task", str(task_id))
            
            # Update fields
            if title is not None:
                if not title.strip():
                    raise ValidationError("title cannot be empty", field="title")
                if len(title) > 500:
                    raise ValidationError("title must be 500 characters or less", field="title")
                task.title = title.strip()
            
            if description is not None:
                task.description = description.strip() if description else None
            
            if status is not None:
                valid_statuses = ['pending', 'in_progress', 'completed', 'overdue', 'cancelled']
                if status not in valid_statuses:
                    raise ValidationError(f"status must be one of {valid_statuses}", field="status")
                task.status = status
                
                # Set completed_at if marking as completed
                if status == 'completed' and not task.completed_at:
                    task.completed_at = datetime.utcnow()
                elif status != 'completed':
                    task.completed_at = None
            
            if priority is not None:
                valid_priorities = ['low', 'medium', 'high', 'urgent']
                if priority not in valid_priorities:
                    raise ValidationError(f"priority must be one of {valid_priorities}", field="priority")
                task.priority = priority
            
            if due_date is not None:
                task.due_date = due_date
                # Update status if overdue
                if due_date < datetime.utcnow() and task.status not in ['completed', 'cancelled']:
                    task.status = 'overdue'
            
            task.updated_at = datetime.utcnow()
            db.flush()
            
            result = {
                'id': task.id,
                'user_id': task.user_id,
                'title': task.title,
                'description': task.description,
                'status': task.status,
                'priority': task.priority,
                'due_date': task.due_date.isoformat() if task.due_date else None,
                'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                'created_at': task.created_at.isoformat() if task.created_at else None,
                'updated_at': task.updated_at.isoformat() if task.updated_at else None
            }
            
            logger.info(f"Successfully updated task {task_id}")
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            logger.error(f"Error updating task: {str(e)}")
            raise DatabaseError(
                f"Failed to update task: {str(e)}",
                operation="update_task"
            )
    
    def delete_task(self, task_id: int, user_id: str, db: Optional[Session] = None):
        """
        Delete a task
        
        Args:
            task_id: Task ID
            user_id: User identifier
            db: Optional database session
        
        Raises:
            NotFoundError: If task not found
        """
        if db is not None:
            self._delete_task_impl(task_id, user_id, db)
        else:
            with get_db_context() as db:
                self._delete_task_impl(task_id, user_id, db)
    
    def _delete_task_impl(self, task_id: int, user_id: str, db: Session):
        """Internal implementation"""
        try:
            task = db.query(Task).filter(
                and_(Task.id == task_id, Task.user_id == user_id)
            ).first()
            
            if not task:
                raise NotFoundError("task", str(task_id))
            
            db.delete(task)
            logger.info(f"Successfully deleted task {task_id}")
        except NotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error deleting task: {str(e)}")
            raise DatabaseError(
                f"Failed to delete task: {str(e)}",
                operation="delete_task"
            )
    
    def get_upcoming_tasks(self, user_id: str, days: int = 7, db: Optional[Session] = None) -> List[Dict]:
        """
        Get tasks with upcoming deadlines
        
        Args:
            user_id: User identifier
            days: Number of days to look ahead
            db: Optional database session
        
        Returns:
            List of tasks with upcoming deadlines
        """
        if db is not None:
            return self._get_upcoming_tasks_impl(user_id, days, db)
        else:
            with get_db_context() as db:
                return self._get_upcoming_tasks_impl(user_id, days, db)
    
    def _get_upcoming_tasks_impl(self, user_id: str, days: int, db: Session) -> List[Dict]:
        """Internal implementation"""
        try:
            now = datetime.utcnow()
            future_date = now + timedelta(days=days)
            
            tasks = db.query(Task).filter(
                and_(
                    Task.user_id == user_id,
                    Task.due_date >= now,
                    Task.due_date <= future_date,
                    Task.status.in_(['pending', 'in_progress'])
                )
            ).order_by(Task.due_date.asc()).all()
            
            return [{
                'id': task.id,
                'title': task.title,
                'description': task.description,
                'status': task.status,
                'priority': task.priority,
                'due_date': task.due_date.isoformat() if task.due_date else None,
                'days_until_due': (task.due_date - now).days if task.due_date else None
            } for task in tasks]
        except Exception as e:
            logger.error(f"Error getting upcoming tasks: {str(e)}")
            raise DatabaseError(
                f"Failed to get upcoming tasks: {str(e)}",
                operation="get_upcoming_tasks"
            )
    
    def check_overdue_tasks(self, user_id: Optional[str] = None, db: Optional[Session] = None) -> List[Dict]:
        """
        Check and update overdue tasks
        
        Args:
            user_id: Optional user identifier (if None, checks all users)
            db: Optional database session
        
        Returns:
            List of overdue tasks
        """
        if db is not None:
            return self._check_overdue_tasks_impl(user_id, db)
        else:
            with get_db_context() as db:
                return self._check_overdue_tasks_impl(user_id, db)
    
    def _check_overdue_tasks_impl(self, user_id: Optional[str], db: Session) -> List[Dict]:
        """Internal implementation"""
        try:
            now = datetime.utcnow()
            query = db.query(Task).filter(
                and_(
                    Task.due_date < now,
                    Task.status.in_(['pending', 'in_progress'])
                )
            )
            
            if user_id:
                query = query.filter(Task.user_id == user_id)
            
            overdue_tasks = query.all()
            
            # Update status to overdue
            result = []
            for task in overdue_tasks:
                task.status = 'overdue'
                result.append({
                    'id': task.id,
                    'user_id': task.user_id,
                    'title': task.title,
                    'due_date': task.due_date.isoformat() if task.due_date else None,
                    'priority': task.priority
                })
            
            db.flush()
            return result
        except Exception as e:
            logger.error(f"Error checking overdue tasks: {str(e)}")
            raise DatabaseError(
                f"Failed to check overdue tasks: {str(e)}",
                operation="check_overdue_tasks"
            )

