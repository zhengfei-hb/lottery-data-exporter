import streamlit as st
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import logging
import io
import time
import hashlib

# 设置页面配置
st.set_page_config(
    page_title="即开票数据查询",
    page_icon="🎫",
    layout="wide",
    initial_sidebar_state="expanded"
)

class LotteryDataExporterStreamlit:
    def __init__(self):
        # 改进的数据库配置
        self.db_config = {
            'host': 'localhost',
            'user': 'zf',
            'password': '117225982',
            'database': 'lottery',
            'charset': 'utf8mb4',
            'port': 3306,
            'connect_timeout': 10,
        }
        
        # 列名映射
        self.column_mapping = {
            'region': '兑奖单位',
            'play_method': '方案名称',
            'sale_site': '售出站点',
            'redeem_site': '兑奖站点',
            'prize_level': '兑奖金额',
            'redeem_time': '兑奖时间',
            'sale_time': '售出时间'
        }
        
        self.table_name = "各奖等中奖明细表"
        self.user_table = "users"
        
        # 初始化 session state
        self.init_session_state()
    
    def init_session_state(self):
        """初始化 session state"""
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'username' not in st.session_state:
            st.session_state.username = None
        if 'selected_play_methods' not in st.session_state:
            st.session_state.selected_play_methods = []
        if 'prize_conditions' not in st.session_state:
            st.session_state.prize_conditions = {}
        if 'preview_data' not in st.session_state:
            st.session_state.preview_data = None
        if 'play_methods_list' not in st.session_state:
            st.session_state.play_methods_list = []
        if 'regions_list' not in st.session_state:
            st.session_state.regions_list = []
        if 'last_query_success' not in st.session_state:
            st.session_state.last_query_success = False
        if 'log_messages' not in st.session_state:
            st.session_state.log_messages = []
        if 'db_connected' not in st.session_state:
            st.session_state.db_connected = False
        if 'methods_loaded' not in st.session_state:
            st.session_state.methods_loaded = False
        if 'regions_loaded' not in st.session_state:
            st.session_state.regions_loaded = False
        if 'initial_load_attempted' not in st.session_state:
            st.session_state.initial_load_attempted = False
        if 'data_update_date' not in st.session_state:
            st.session_state.data_update_date = None
    
    def get_latest_redeem_date(self):
        """从数据库获取最新的兑奖日期"""
        try:
            if not self.test_db_connection():
                return None
                
            connection = pymysql.connect(**self.db_config)
            cursor = connection.cursor()
            
            redeem_time_col = self.column_mapping['redeem_time']
            query = f"SELECT MAX({redeem_time_col}) FROM {self.table_name}"
            cursor.execute(query)
            result = cursor.fetchone()
            
            connection.close()
            
            if result and result[0]:
                latest_date = result[0]
                if isinstance(latest_date, datetime):
                    return latest_date.date()
                elif isinstance(latest_date, str):
                    # 如果是字符串格式，尝试解析
                    try:
                        return datetime.strptime(latest_date, '%Y-%m-%d').date()
                    except:
                        return datetime.strptime(latest_date, '%Y/%m/%d').date()
                else:
                    return latest_date
            return None
            
        except Exception as e:
            self.log_message(f"获取最新兑奖日期失败: {e}")
            return None
    
    def hash_password(self, password):
        """对密码进行哈希处理"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def test_db_connection(self):
        """测试数据库连接"""
        try:
            connection = pymysql.connect(**self.db_config)
            connection.close()
            st.session_state.db_connected = True
            return True
        except Exception as e:
            st.session_state.db_connected = False
            return False
    
    def verify_user(self, username, password):
        """验证用户登录信息"""
        if not self.test_db_connection():
            return False
            
        try:
            connection = pymysql.connect(**self.db_config)
            cursor = connection.cursor()
            
            hashed_password = self.hash_password(password)
            query = f"SELECT * FROM {self.user_table} WHERE username = %s AND password = %s"
            cursor.execute(query, (username, hashed_password))
            user = cursor.fetchone()
            
            connection.close()
            
            if user:
                # 登录成功后获取最新数据日期
                st.session_state.data_update_date = self.get_latest_redeem_date()
                return True
            else:
                return False
                
        except Exception as e:
            return False
    
    def setup_login_ui(self):
        """设置登录界面"""
        st.title("🎫 即开票兑奖数据导出V1.0.1")
        st.markdown("---")
        
        with st.form("login_form"):
            st.subheader("用户登录")
            
            username = st.text_input("用户名", placeholder="请输入用户名")
            password = st.text_input("密码", type="password", placeholder="请输入密码")
            
            col1, col2, col3 = st.columns([2, 1, 2])
            with col2:
                login_button = st.form_submit_button("🚪 登录", use_container_width=True)
            
            if login_button:
                if not username or not password:
                    st.error("请输入用户名和密码")
                else:
                    with st.spinner("正在验证用户信息..."):
                        if self.verify_user(username, password):
                            st.session_state.authenticated = True
                            st.session_state.username = username
                            st.success(f"欢迎 {username}！")
                            self.log_message(f"用户 {username} 登录成功")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("用户名或密码错误，或系统连接异常")
                            self.log_message(f"登录失败 - 用户名: {username}")
    
    def setup_main_ui(self):
        """设置主界面（查询页面）"""
        # 顶部导航栏
        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            # 显示标题和数据更新日期
            title_text = "🎫 即开票兑奖数据导出V1.0.1"
            if st.session_state.data_update_date:
                title_text += f" (数据更新至: {st.session_state.data_update_date})"
            st.title(title_text)
        with col2:
            st.write(f"**欢迎, {st.session_state.username}**")
        with col3:
            if st.button("🚪 退出登录"):
                st.session_state.authenticated = False
                st.session_state.username = None
                st.session_state.data_update_date = None
                st.session_state.log_messages.clear()
                st.rerun()
        
        st.markdown("---")
        
        # 侧边栏 - 系统信息
        with st.sidebar:
            st.header("👤 用户信息")
            st.write(f"用户名: {st.session_state.username}")
            st.write(f"登录时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 显示数据更新日期
            if st.session_state.data_update_date:
                st.write(f"数据更新至: **{st.session_state.data_update_date}**")
            
            st.markdown("---")
            st.header("⚙️ 系统配置")
            
            # 数据库连接状态显示
            if st.session_state.db_connected:
                st.success("✅ 数据库已连接")
            else:
                st.error("❌ 数据库未连接")
            
            # 数据加载状态
            if st.session_state.methods_loaded:
                st.success(f"✅ 已加载 {len(st.session_state.play_methods_list)} 种玩法")
            else:
                st.warning("⚠️ 玩法数据未加载")
            
            if st.session_state.regions_loaded:
                st.success(f"✅ 已加载 {len(st.session_state.regions_list)} 个单位")
            else:
                st.warning("⚠️ 单位数据未加载")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔗 测试连接", use_container_width=True):
                    if self.test_db_connection():
                        st.success("✅ 数据库连接成功！")
                    else:
                        st.error("❌ 数据库连接失败")
            with col2:
                if st.button("🔄 刷新数据", use_container_width=True):
                    self.refresh_data_lists()
            
            st.markdown("---")
            st.header("📊 快速操作")
            if st.button("🗑️ 清空所有条件", use_container_width=True):
                self.clear_all_conditions()
            
            # 显示系统状态
            st.markdown("---")
            st.header("📈 系统状态")
            st.metric("已选玩法", len(st.session_state.selected_play_methods))
            st.metric("金额条件", len(st.session_state.prize_conditions))
            if st.session_state.preview_data is not None:
                st.metric("查询结果", len(st.session_state.preview_data))
        
        # 主内容区 - 使用标签页组织
        tab1, tab2, tab3, tab4 = st.tabs(["🔍 数据筛选", "📋 数据预览", "💾 数据导出", "📝 操作日志"])
        
        with tab1:
            self.setup_filter_ui()
        
        with tab2:
            self.setup_preview_ui()
        
        with tab3:
            self.setup_export_ui()
        
        with tab4:
            self.setup_log_ui()
    
    def refresh_data_lists(self):
        """刷新玩法和单位列表"""
        try:
            if not self.test_db_connection():
                st.sidebar.error("❌ 数据库未连接，无法刷新数据")
                return
                
            with st.spinner("正在刷新数据..."):
                success1 = self.fetch_play_methods_from_db()
                success2 = self.fetch_regions_from_db()
                # 同时刷新数据更新日期
                st.session_state.data_update_date = self.get_latest_redeem_date()
            
            if success1 and success2:
                st.sidebar.success("✅ 数据列表刷新成功")
            else:
                st.sidebar.error("❌ 数据刷新失败")
        except Exception as e:
            st.sidebar.error("刷新失败，请检查系统连接")

    # ... 其余方法保持不变，只修改了标题显示部分 ...

    def setup_filter_ui(self):
        """设置筛选条件界面"""
        st.header("🔍 数据筛选条件")
        
        # 显示数据更新日期（如果可用）
        if st.session_state.data_update_date:
            st.info(f"📅 当前数据更新至: **{st.session_state.data_update_date}**")
        
        # 使用列布局
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("🎯 基本条件")
            
            # 兑奖单位选择
            st.write("**🏢 兑奖单位**")
            region_col1, region_col2 = st.columns([3, 1])
            with region_col1:
                selected_region = st.selectbox(
                    "选择兑奖单位",
                    options=[""] + st.session_state.regions_list,
                    key="region_select",
                    label_visibility="collapsed"
                )
            with region_col2:
                if st.button("📥 获取单位", key="fetch_regions", use_container_width=True):
                    with st.spinner("正在获取兑奖单位..."):
                        if self.fetch_regions_from_db():
                            st.success("✅ 兑奖单位列表已更新")
                        else:
                            st.error("❌ 获取兑奖单位失败")
            
            # 兑奖站点
            redeem_site = st.text_input("🏪 兑奖站点", key="redeem_site")
            
            # 玩法管理
            st.write("**🎮 玩法管理**")
            method_col1, method_col2 = st.columns([3, 1])
            with method_col1:
                selected_method = st.selectbox(
                    "选择玩法",
                    options=[""] + st.session_state.play_methods_list,
                    key="method_select",
                    label_visibility="collapsed"
                )
            with method_col2:
                if st.button("📥 获取玩法", key="fetch_methods", use_container_width=True):
                    with st.spinner("正在获取玩法列表..."):
                        if self.fetch_play_methods_from_db():
                            st.success("✅ 玩法列表已更新")
                        else:
                            st.error("❌ 获取玩法列表失败")
            
            # 添加玩法按钮
            if selected_method and selected_method not in st.session_state.selected_play_methods:
                if st.button("➕ 添加玩法", key="add_method", use_container_width=True):
                    st.session_state.selected_play_methods.append(selected_method)
                    self.log_message(f"已添加玩法: {selected_method}")
                    st.rerun()
            
            # 显示已选玩法列表
            if st.session_state.selected_play_methods:
                st.write("**✅ 已选玩法列表:**")
                for i, method in enumerate(st.session_state.selected_play_methods):
                    method_col1, method_col2 = st.columns([4, 1])
                    method_col1.write(f"• {method}")
                    if method_col2.button("🗑️", key=f"remove_{i}", help=f"移除 {method}"):
                        removed_method = st.session_state.selected_play_methods.pop(i)
                        if removed_method in st.session_state.prize_conditions:
                            del st.session_state.prize_conditions[removed_method]
                        self.log_message(f"已移除玩法: {removed_method}")
                        st.rerun()
                
                if st.button("🗑️ 清空所有玩法", key="clear_all_methods"):
                    st.session_state.selected_play_methods.clear()
                    st.session_state.prize_conditions.clear()
                    self.log_message("已清空所有玩法和金额条件")
                    st.rerun()
            else:
                st.info("ℹ️ 尚未选择任何玩法")
        
        with col2:
            st.subheader("⚡ 高级条件")
            
            # 兑奖金额条件
            st.write("**💰 兑奖金额条件**")
            if st.session_state.selected_play_methods:
                for method in st.session_state.selected_play_methods:
                    amount = st.text_input(
                        f"{method} - 金额条件",
                        value=st.session_state.prize_conditions.get(method, ""),
                        key=f"amount_{method}",
                        placeholder="输入金额，如: 100"
                    )
                    if amount:
                        st.session_state.prize_conditions[method] = amount
                    elif method in st.session_state.prize_conditions and not amount:
                        del st.session_state.prize_conditions[method]
                
                # 快速金额设置
                st.write("**🚀 快速设置**")
                quick_amount_cols = st.columns(5)
                common_amounts = ["5", "10", "50", "100", "500"]
                for i, amount in enumerate(common_amounts):
                    if quick_amount_cols[i].button(f"{amount}元", key=f"quick_{amount}"):
                        for method in st.session_state.selected_play_methods:
                            st.session_state.prize_conditions[method] = amount
                        self.log_message(f"已为所有玩法设置金额: {amount}元")
                        st.rerun()
            else:
                st.info("ℹ️ 请先选择玩法以设置金额条件")
            
            # 时间筛选条件
            st.write("**⏰ 时间范围**")
            
            time_col1, time_col2 = st.columns(2)
            with time_col1:
                use_redeem_time = st.checkbox("启用兑奖时间筛选", key="use_redeem_time")
            with time_col2:
                use_sale_time = st.checkbox("启用销售时间筛选", key="use_sale_time")
            
            if use_redeem_time:
                redeem_col1, redeem_col2 = st.columns(2)
                with redeem_col1:
                    redeem_start = st.date_input(
                        "兑奖开始时间", 
                        value=datetime(2025, 1, 1),
                        key="redeem_start"
                    )
                with redeem_col2:
                    redeem_end = st.date_input(
                        "兑奖结束时间", 
                        value=datetime(2025, 12, 31),
                        key="redeem_end"
                    )
            
            if use_sale_time:
                sale_col1, sale_col2 = st.columns(2)
                with sale_col1:
                    sale_start = st.date_input(
                        "销售开始时间", 
                        value=datetime(2025, 1, 1),
                        key="sale_start"
                    )
                with sale_col2:
                    sale_end = st.date_input(
                        "销售结束时间", 
                        value=datetime(2025, 12, 31),
                        key="sale_end"
                    )
            
            # 快速时间设置
            st.write("**📅 快速时间设置**")
            time_buttons_cols = st.columns(5)
            with time_buttons_cols[0]:
                if st.button("今天", use_container_width=True):
                    self.set_today()
            with time_buttons_cols[1]:
                if st.button("最近7天", use_container_width=True):
                    self.set_last_7_days()
            with time_buttons_cols[2]:
                if st.button("最近30天", use_container_width=True):
                    self.set_last_30_days()
            with time_buttons_cols[3]:
                if st.button("本月", use_container_width=True):
                    self.set_this_month()
            with time_buttons_cols[4]:
                if st.button("上个月", use_container_width=True):
                    self.set_last_month()
        
        # 操作按钮
        st.markdown("---")
        action_col1, action_col2, action_col3, action_col4 = st.columns(4)
        
        with action_col1:
            if st.button("🚀 预览数据", use_container_width=True, type="primary"):
                self.preview_data_func()
        
        with action_col2:
            if st.button("💾 导出数据", use_container_width=True):
                self.export_data()
        
        with action_col3:
            if st.button("🔄 重置条件", use_container_width=True):
                self.clear_filter_conditions()
        
        with action_col4:
            if st.button("📊 查看统计", use_container_width=True):
                self.show_statistics()

    # ... 其余方法保持不变 ...

# 运行应用
def main():
    app = LotteryDataExporterStreamlit()
    app.setup_ui()

if __name__ == "__main__":
    main()