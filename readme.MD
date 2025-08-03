# 🎓 Student Branch Recommendation System

This project is built using **FastAPI** and **SQL Server**. It enables student registration, subject and marks recording, and provides academic branch recommendations based on subject-wise marks and qualifying exam results.

---

## 🚀 Features

- Register new students with their subject-wise marks.
- Automatically generate a unique roll number.
- Validate and store subjects and marks in a relational database.
- Recommend eligible academic branches based on configurable criteria.
- RESTful API endpoints documented using Swagger UI and ReDoc.

---

## 📁 Project Structure
├── app.py # Main FastAPI application file
├── requirements.txt # Project dependencies
├── README.md # Project documentation

---

### 1. Clone the Repository
- git clone https://github.com/ssadasivmohanty/student_recomendation_portal.git
- cd student_recomendation_portal

### 2. Create a Virtual Environment
- python -m venv venv

### 3. Activate the Virtual Environment
#### On Windows:
    venv\Scripts\activate
#### On macOS/Linux:
    source venv/bin/activate

### 4. Install Dependencies
    pip install -r requirements.txt

## 🏁 Running the Application
### Start the FastAPI server using Uvicorn:
    uvicorn app:app --reload

#### The app will be accessible at:
🔗 http://127.0.0.1:8000