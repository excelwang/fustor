from fastapi import FastAPI, APIRouter, Request
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import asyncio
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
import os

from .database import register_engine, get_db
from .models import StateBase, UserModel, UserLevel
from .security import hash_password
from .config import register_config
from .admin.router import router as admin_router
from .profile.router import router as profile_router
from .auth.router import router as auth_router
from .apikey.router import router as apikey_router
from .datastore.router import router as datastores_router
from .api.internal.keys_api import internal_keys_router # CORRECTED

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    异步初始化数据库和管理员用户
    """
    try:
        if not register_config.FUSTOR_REGISTRY_DB_URL:
            print("FUSTOR_REGISTRY_DB_URL not set, skipping database initialization.")
            yield
            return

        # 使用共享的 state_engine 创建表
        async with register_engine.begin() as conn:
            await conn.run_sync(StateBase.metadata.create_all)
        
        # 创建异步会话工厂
        async_session = async_sessionmaker(
            bind=register_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        # 创建异步会话
        async with async_session() as db:
            # 检查admin用户
            result = await db.execute(select(UserModel).where(UserModel.email == "admin@admin.com"))
            admin_user = result.scalars().first()
            
            if not admin_user:
                # 调试日志
                print(f"正在初始化管理员用户，使用数据库: {register_config.FUSTOR_REGISTRY_DB_URL}")
                # 创建admin用户
                hashed_password = hash_password("admin")
                
                admin_user = UserModel(
                    name="Admin",
                    email="admin@admin.com",
                    password=hashed_password,
                    level=UserLevel.ADMIN,
                    is_active=True
                )
                
                db.add(admin_user)
                await db.commit()
                await db.refresh(admin_user)
                print(f"管理员用户 {admin_user.email} 已创建")
    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        # 开发时打印完整错误
        import traceback
        traceback.print_exc()
    
    yield


app = FastAPI(
    title="Fusion Storage Engine Register Web API",
    description="Web API for managing datastores registerations.",
    version="0.1.0",
    lifespan=lifespan,
)

router_v1 = APIRouter(prefix="/v1", tags=["v1"])
router_v1.include_router(profile_router)
router_v1.include_router(auth_router)
router_v1.include_router(apikey_router)
router_v1.include_router(datastores_router)
router_v1.include_router(admin_router)

app.include_router(router_v1)
app.include_router(internal_keys_router, prefix="/internal", tags=["Internal"]) # NEW: Include internal router

ui_dir = os.path.join(os.path.dirname(__file__), "ui")
app.mount("/assets", StaticFiles(directory=f"{ui_dir}/assets", html=True), name="assets")
@app.get("/", tags=["UI"]) 
async def read_web_api_root(request: Request):
    # 返回 UI 入口文件
    return FileResponse(f"{ui_dir}/index.html")
