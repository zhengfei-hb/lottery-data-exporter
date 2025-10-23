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
        if 'site_analysis_data' not in st.session_state:
            st.session_state.site_analysis_data = None
    
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
                        try:
                            return datetime.strptime(latest_date, '%Y/%m/%d').date()
                        except:
                            # 尝试其他可能的格式
                            try:
                                return datetime.strptime(latest_date.split()[0], '%Y-%m-%d').date()
                            except:
                                return None
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
                latest_date = self.get_latest_redeem_date()
                if latest_date:
                    st.session_state.data_update_date = latest_date
                    self.log_message(f"获取到最新数据日期: {latest_date}")
                else:
                    st.session_state.data_update_date = None
                    self.log_message("未获取到数据更新日期")
                return True
            else:
                return False
                
        except Exception as e:
            self.log_message(f"用户验证失败: {e}")
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
            if st.session_state.data_update_date:
                st.title(f"🎫 即开票兑奖数据导出V1.0.1 (数据更新至: {st.session_state.data_update_date})")
            else:
                st.title("🎫 即开票兑奖数据导出V1.0.1")
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
            else:
                st.write("数据更新至: **未知**")
            
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
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["🔍 数据筛选", "📋 数据预览", "🏪 站点分析", "💾 数据导出", "📝 操作日志"])
        
        with tab1:
            self.setup_filter_ui()
        
        with tab2:
            self.setup_preview_ui()
        
        with tab3:
            self.setup_site_analysis_ui()
        
        with tab4:
            self.setup_export_ui()
        
        with tab5:
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
                latest_date = self.get_latest_redeem_date()
                if latest_date:
                    st.session_state.data_update_date = latest_date
                    st.sidebar.success(f"✅ 数据已刷新，最新日期: {latest_date}")
                else:
                    st.session_state.data_update_date = None
                    st.sidebar.warning("⚠️ 数据刷新成功，但未获取到最新日期")
            
            if success1 and success2:
                self.log_message("数据列表刷新成功")
            else:
                self.log_message("数据刷新失败")
        except Exception as e:
            st.sidebar.error("刷新失败，请检查系统连接")
    
    def setup_ui(self):
        """设置用户界面"""
        if not st.session_state.authenticated:
            self.setup_login_ui()
        else:
            self.setup_main_ui()
    
    def setup_filter_ui(self):
        """设置筛选条件界面"""
        st.header("🔍 数据筛选条件")
        
        # 显示数据更新日期（如果可用）
        if st.session_state.data_update_date:
            st.info(f"📅 当前数据更新至: **{st.session_state.data_update_date}**")
        else:
            st.warning("⚠️ 未获取到数据更新日期")
        
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
        action_col1, action_col2, action_col3, action_col4, action_col5 = st.columns(5)
        
        with action_col1:
            if st.button("🚀 预览数据", use_container_width=True, type="primary"):
                self.preview_data_func()
        
        with action_col2:
            if st.button("🏪 站点分析", use_container_width=True):
                self.analyze_site_data()
        
        with action_col3:
            if st.button("💾 导出数据", use_container_width=True):
                self.export_data()
        
        with action_col4:
            if st.button("🔄 重置条件", use_container_width=True):
                self.clear_filter_conditions()
        
        with action_col5:
            if st.button("📊 查看统计", use_container_width=True):
                self.show_statistics()
    
    def setup_preview_ui(self):
        """设置数据预览界面"""
        st.header("📋 数据预览")
        
        if st.session_state.preview_data is not None:
            if not st.session_state.preview_data.empty:
                st.success(f"✅ 查询到 {len(st.session_state.preview_data)} 条记录")
                
                # 数据显示选项
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    show_count = st.slider("显示记录数量", 10, 10000, 1000, 10)
                with col2:
                    show_all = st.checkbox("显示所有列")
                with col3:
                    if st.button("刷新预览"):
                        st.rerun()
                
                # 显示数据
                display_data = st.session_state.preview_data.head(show_count)
                if not show_all and len(display_data.columns) > 10:
                    cols_to_show = list(display_data.columns[:5]) + list(display_data.columns[-5:])
                    display_data = display_data[cols_to_show]
                    st.info("显示前5列和后5列，勾选'显示所有列'查看完整数据")
                
                st.dataframe(display_data, use_container_width=True)
                
            else:
                st.warning("⚠️ 没有找到符合条件的数据")
                st.info("请调整筛选条件后重新查询")
        else:
            st.info("ℹ️ 请先在「数据筛选」标签页中设置条件并点击「预览数据」")
    
    def setup_site_analysis_ui(self):
        """设置站点分析界面"""
        st.header("🏪 售出站点与兑奖站点分析")
        
        if st.session_state.site_analysis_data is not None:
            if not st.session_state.site_analysis_data.empty:
                st.success(f"✅ 分析数据已生成，共 {len(st.session_state.site_analysis_data)} 条记录")
                
                # 分析选项
                col1, col2 = st.columns(2)
                with col1:
                    analysis_type = st.radio(
                        "分析类型",
                        ["全部", "站点一致", "站点不一致"],
                        horizontal=True
                    )
                with col2:
                    if st.button("🔄 刷新分析"):
                        self.analyze_site_data()
                
                # 筛选数据
                if analysis_type == "站点一致":
                    analysis_data = st.session_state.site_analysis_data[
                        st.session_state.site_analysis_data['站点关系'] == '一致'
                    ]
                elif analysis_type == "站点不一致":
                    analysis_data = st.session_state.site_analysis_data[
                        st.session_state.site_analysis_data['站点关系'] == '不一致'
                    ]
                else:
                    analysis_data = st.session_state.site_analysis_data
                
                st.subheader(f"📊 {analysis_type}情况统计")
                
                # 统计信息
                if not analysis_data.empty:
                    # 按区域统计
                    region_stats = analysis_data.groupby(['兑奖单位', '站点关系']).agg({
                        '兑奖金额': ['count', 'sum']
                    }).round(2)
                    region_stats.columns = ['记录数', '总金额']
                    region_stats = region_stats.reset_index()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**📈 按区域统计**")
                        st.dataframe(region_stats, use_container_width=True)
                    
                    with col2:
                        st.write("**🎯 关键指标**")
                        
                        total_records = len(analysis_data)
                        total_amount = analysis_data['兑奖金额'].sum()
                        avg_amount = analysis_data['兑奖金额'].mean()
                        
                        st.metric("总记录数", f"{total_records:,}")
                        st.metric("总兑奖金额", f"¥{total_amount:,.2f}")
                        st.metric("平均兑奖金额", f"¥{avg_amount:,.2f}")
                        
                        # 站点关系分布
                        if analysis_type == "全部":
                            site_relation_stats = analysis_data['站点关系'].value_counts()
                            st.write("**🔗 站点关系分布**")
                            for relation, count in site_relation_stats.items():
                                st.write(f"- {relation}: {count} 条 ({count/total_records*100:.1f}%)")
                    
                    # 详细数据展示
                    st.subheader("📋 详细数据")
                    
                    show_count = st.slider("显示记录数量", 10, 1000, 100, 10, key="analysis_show_count")
                    display_analysis_data = analysis_data.head(show_count)
                    
                    st.dataframe(display_analysis_data, use_container_width=True)
                    
                    # 导出分析结果
                    st.subheader("💾 导出分析结果")
                    export_filename = st.text_input(
                        "导出文件名",
                        value=f"site_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        key="analysis_export_filename"
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("📊 导出分析数据", use_container_width=True):
                            self.export_analysis_data(analysis_data, export_filename)
                    with col2:
                        if st.button("📈 导出统计报表", use_container_width=True):
                            self.export_statistics_report(region_stats, export_filename)
                
                else:
                    st.warning(f"⚠️ 没有找到{analysis_type}的数据")
            
            else:
                st.warning("⚠️ 没有找到符合条件的数据")
                st.info("请调整筛选条件后重新分析")
        else:
            st.info("ℹ️ 请先在「数据筛选」标签页中设置条件并点击「站点分析」")
    
    def setup_export_ui(self):
        """设置数据导出界面"""
        st.header("💾 数据导出")
        
        if st.session_state.preview_data is not None and not st.session_state.preview_data.empty:
            st.success(f"✅ 当前有 {len(st.session_state.preview_data)} 条数据可导出")
            
            # 导出设置
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("导出设置")
                export_format = st.radio(
                    "导出格式",
                    ["Excel", "CSV"],
                    horizontal=True
                )
                
                filename = st.text_input(
                    "文件名",
                    value=f"lottery_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                
                if export_format == "Excel":
                    include_index = st.checkbox("包含行索引", value=False)
                else:
                    encoding = st.selectbox("编码格式", ["utf-8", "gbk", "utf-8-sig"])
            
            with col2:
                st.subheader("导出操作")
                
                if export_format == "Excel":
                    if st.button("📥 下载 Excel 文件", use_container_width=True, type="primary"):
                        self.download_excel(filename, include_index)
                else:
                    if st.button("📥 下载 CSV 文件", use_container_width=True, type="primary"):
                        self.download_csv(filename, encoding)
                
                # 导出统计信息
                st.metric("总记录数", len(st.session_state.preview_data))
                st.metric("数据列数", len(st.session_state.preview_data.columns))
                
                # 预览导出数据
                with st.expander("👀 预览导出数据（前5行）"):
                    st.dataframe(st.session_state.preview_data.head(), use_container_width=True)
        
        else:
            st.warning("⚠️ 没有可导出的数据")
            st.info("请先查询数据后再进行导出操作")
    
    def setup_log_ui(self):
        """设置操作日志界面"""
        st.header("📝 操作日志")
        
        # 日志控制
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("**最近操作记录:**")
        with col2:
            if st.button("清空日志", use_container_width=True):
                st.session_state.log_messages.clear()
                st.rerun()
        
        # 显示日志
        log_container = st.container()
        with log_container:
            if st.session_state.log_messages:
                for log_entry in reversed(st.session_state.log_messages[-20:]):
                    st.text(log_entry)
            else:
                st.info("暂无日志记录")
    
    def analyze_site_data(self):
        """分析售出站点与兑奖站点数据"""
        try:
            if st.session_state.preview_data is None or st.session_state.preview_data.empty:
                st.warning("⚠️ 请先预览数据再进行站点分析")
                return
            
            self.log_message("开始分析站点数据...")
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("正在处理数据...")
            progress_bar.progress(30)
            
            # 获取列名映射
            sale_site_col = self.column_mapping['sale_site']
            redeem_site_col = self.column_mapping['redeem_site']
            region_col = self.column_mapping['region']
            prize_level_col = self.column_mapping['prize_level']
            
            # 创建分析数据副本
            analysis_data = st.session_state.preview_data.copy()
            
            status_text.text("分析站点关系...")
            progress_bar.progress(60)
            
            # 分析站点关系
            analysis_data['站点关系'] = analysis_data.apply(
                lambda row: '一致' if str(row[sale_site_col]) == str(row[redeem_site_col]) else '不一致', 
                axis=1
            )
            
            # 重命名列以便显示
            analysis_data = analysis_data.rename(columns={
                region_col: '兑奖单位',
                sale_site_col: '售出站点',
                redeem_site_col: '兑奖站点',
                prize_level_col: '兑奖金额'
            })
            
            # 选择需要显示的列
            display_columns = ['兑奖单位', '售出站点', '兑奖站点', '站点关系', '兑奖金额']
            # 添加其他可能需要的列
            for col in ['方案名称', '兑奖时间', '售出时间']:
                if col in analysis_data.columns:
                    display_columns.append(col)
            
            analysis_data = analysis_data[display_columns]
            
            status_text.text("完成分析...")
            progress_bar.progress(90)
            
            st.session_state.site_analysis_data = analysis_data
            
            progress_bar.progress(100)
            status_text.text("分析完成！")
            time.sleep(0.5)
            status_text.empty()
            progress_bar.empty()
            
            self.log_message(f"站点分析完成，共分析 {len(analysis_data)} 条记录")
            st.success("✅ 站点分析完成！")
            
        except Exception as e:
            error_msg = f"站点分析过程中发生错误: {e}"
            st.error("站点分析过程中发生错误，请重试")
            self.log_message(f"站点分析失败: {e}")
    
    def export_analysis_data(self, data, filename):
        """导出分析数据"""
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                data.to_excel(writer, index=False, sheet_name='站点分析数据')
            
            st.download_button(
                label="📥 点击下载分析数据",
                data=buffer.getvalue(),
                file_name=f"{filename}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="analysis_download"
            )
            
            self.log_message(f"站点分析数据已准备下载: {filename}.xlsx")
            
        except Exception as e:
            st.error(f"导出分析数据失败: {e}")
            self.log_message(f"导出分析数据失败: {e}")
    
    def export_statistics_report(self, stats_data, filename):
        """导出统计报表"""
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                stats_data.to_excel(writer, index=False, sheet_name='统计报表')
            
            st.download_button(
                label="📥 点击下载统计报表",
                data=buffer.getvalue(),
                file_name=f"{filename}_stats.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="stats_download"
            )
            
            self.log_message(f"统计报表已准备下载: {filename}_stats.xlsx")
            
        except Exception as e:
            st.error(f"导出统计报表失败: {e}")
            self.log_message(f"导出统计报表失败: {e}")
    
    def download_excel(self, filename, include_index=False):
        """下载Excel文件"""
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                st.session_state.preview_data.to_excel(
                    writer, 
                    index=include_index, 
                    sheet_name='彩票数据'
                )
            
            st.download_button(
                label="📥 点击下载 Excel 文件",
                data=buffer.getvalue(),
                file_name=f"{filename}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_download"
            )
            
            self.log_message(f"Excel文件已准备下载: {filename}.xlsx")
            
        except Exception as e:
            st.error(f"导出Excel失败: {e}")
            self.log_message(f"导出Excel失败: {e}")
    
    def download_csv(self, filename, encoding="utf-8"):
        """下载CSV文件"""
        try:
            csv_data = st.session_state.preview_data.to_csv(index=False, encoding=encoding)
            
            st.download_button(
                label="📥 点击下载 CSV 文件",
                data=csv_data,
                file_name=f"{filename}.csv",
                mime="text/csv",
                key="csv_download"
            )
            
            self.log_message(f"CSV文件已准备下载: {filename}.csv")
            
        except Exception as e:
            st.error(f"导出CSV失败: {e}")
            self.log_message(f"导出CSV失败: {e}")
    
    def fetch_regions_from_db(self):
        """从数据库获取兑奖单位列表"""
        try:
            if not self.test_db_connection():
                st.error("❌ 数据库未连接")
                return False
                
            connection = pymysql.connect(**self.db_config)
            
            cursor = connection.cursor()
            region_col = self.column_mapping['region']
            cursor.execute(f"SELECT DISTINCT {region_col} FROM {self.table_name} WHERE {region_col} IS NOT NULL AND {region_col} != '' ORDER BY {region_col}")
            results = cursor.fetchall()
            
            st.session_state.regions_list = [result[0] for result in results]
            connection.close()
            
            st.session_state.regions_loaded = True
            self.log_message(f"从数据库获取到 {len(st.session_state.regions_list)} 个兑奖单位")
            return True
            
        except Exception as e:
            st.session_state.regions_loaded = False
            self.log_message("获取兑奖单位列表失败")
            return False
    
    def fetch_play_methods_from_db(self):
        """从数据库获取玩法列表"""
        try:
            if not self.test_db_connection():
                st.error("❌ 数据库未连接")
                return False
                
            connection = pymysql.connect(**self.db_config)
            
            cursor = connection.cursor()
            play_method_col = self.column_mapping['play_method']
            cursor.execute(f"SELECT DISTINCT {play_method_col} FROM {self.table_name} WHERE {play_method_col} IS NOT NULL AND {play_method_col} != ''")
            results = cursor.fetchall()
            
            st.session_state.play_methods_list = [result[0] for result in results]
            connection.close()
            
            st.session_state.methods_loaded = True
            self.log_message(f"从数据库获取到 {len(st.session_state.play_methods_list)} 种玩法")
            return True
            
        except Exception as e:
            st.session_state.methods_loaded = False
            self.log_message("获取玩法列表失败")
            return False
    
    def get_conditions(self):
        """获取筛选条件"""
        conditions = {}
        
        # 兑奖单位条件
        if st.session_state.get('region_select'):
            conditions['region'] = st.session_state.region_select
        
        # 兑奖站点条件
        if st.session_state.get('redeem_site'):
            conditions['redeem_site'] = st.session_state.redeem_site
        
        # 玩法条件
        if st.session_state.selected_play_methods:
            conditions['play_methods'] = st.session_state.selected_play_methods.copy()
        
        # 兑奖金额条件
        if st.session_state.prize_conditions:
            conditions['prize_conditions'] = st.session_state.prize_conditions.copy()
        
        # 时间条件
        if st.session_state.get('use_redeem_time'):
            redeem_start = st.session_state.get('redeem_start')
            redeem_end = st.session_state.get('redeem_end')
            if redeem_start and redeem_end:
                conditions['redeem_start_time'] = redeem_start.strftime('%Y/%m/%d')
                conditions['redeem_end_time'] = redeem_end.strftime('%Y/%m/%d')
        
        if st.session_state.get('use_sale_time'):
            sale_start = st.session_state.get('sale_start')
            sale_end = st.session_state.get('sale_end')
            if sale_start and sale_end:
                conditions['sale_start_time'] = sale_start.strftime('%Y/%m/%d')
                conditions['sale_end_time'] = sale_end.strftime('%Y/%m/%d')
        
        return conditions
    
    def build_query(self, conditions):
        """构建SQL查询语句"""
        base_query = f"SELECT * FROM {self.table_name} WHERE 1=1"
        query_params = []
        
        # 使用正确的列名映射
        region_col = self.column_mapping['region']
        sale_site_col = self.column_mapping['sale_site']
        redeem_site_col = self.column_mapping['redeem_site']
        play_method_col = self.column_mapping['play_method']
        prize_level_col = self.column_mapping['prize_level']
        redeem_time_col = self.column_mapping['redeem_time']
        sale_time_col = self.column_mapping['sale_time']
        
        # 兑奖单位条件
        if conditions.get('region'):
            base_query += f" AND {region_col} = %s"
            query_params.append(conditions['region'])
        
        # 兑奖站点条件
        if conditions.get('redeem_site'):
            base_query += f" AND {redeem_site_col} = %s"
            query_params.append(conditions['redeem_site'])
        
        # 玩法条件（多选）
        if conditions.get('play_methods'):
            placeholders = ', '.join(['%s'] * len(conditions['play_methods']))
            base_query += f" AND {play_method_col} IN ({placeholders})"
            query_params.extend(conditions['play_methods'])
        
        # 兑奖金额条件（按票种设置）
        if conditions.get('prize_conditions'):
            prize_conditions = conditions['prize_conditions']
            if prize_conditions:
                prize_conditions_parts = []
                for method, amount in prize_conditions.items():
                    prize_conditions_parts.append(f"({play_method_col} = %s AND {prize_level_col} = %s)")
                    query_params.extend([method, amount])
                
                base_query += " AND (" + " OR ".join(prize_conditions_parts) + ")"
        
        # 兑奖时间条件
        if conditions.get('redeem_start_time') and conditions.get('redeem_end_time'):
            base_query += f" AND DATE({redeem_time_col}) BETWEEN %s AND %s"
            query_params.extend([conditions['redeem_start_time'], conditions['redeem_end_time']])
        
        # 销售时间条件
        if conditions.get('sale_start_time') and conditions.get('sale_end_time'):
            base_query += f" AND DATE({sale_time_col}) BETWEEN %s AND %s"
            query_params.extend([conditions['sale_start_time'], conditions['sale_end_time']])
        
        return base_query, query_params
    
    def preview_data_func(self):
        """预览数据"""
        try:
            # 检查数据库连接
            if not self.test_db_connection():
                st.error("❌ 数据库未连接，请检查数据库服务")
                return
                
            conditions = self.get_conditions()
            
            self.log_message("开始查询数据...")
            self.log_message(f"筛选条件: {conditions}")
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("正在连接数据库...")
            progress_bar.progress(20)
            
            connection = pymysql.connect(**self.db_config)
            
            status_text.text("构建查询语句...")
            progress_bar.progress(40)
            
            query, params = self.build_query(conditions)
            self.log_message(f"执行查询: {query}")
            self.log_message(f"查询参数: {params}")
            
            status_text.text("执行查询...")
            progress_bar.progress(60)
            
            # 执行查询
            st.session_state.preview_data = pd.read_sql(query, connection, params=params)
            connection.close()
            
            status_text.text("处理查询结果...")
            progress_bar.progress(80)
            
            if st.session_state.preview_data.empty:
                st.session_state.last_query_success = False
                self.log_message("没有找到符合条件的数据")
            else:
                st.session_state.last_query_success = True
                self.log_message(f"查询到 {len(st.session_state.preview_data)} 条记录")
            
            progress_bar.progress(100)
            status_text.text("查询完成！")
            time.sleep(0.5)
            status_text.empty()
            progress_bar.empty()
            
        except Exception as e:
            error_msg = f"查询过程中发生错误: {e}"
            st.error("查询过程中发生错误，请重试")
            self.log_message("查询过程中发生错误")
            st.session_state.last_query_success = False
    
    def export_data(self):
        """导出数据"""
        if st.session_state.preview_data is None or st.session_state.preview_data.empty:
            st.warning("⚠️ 请先预览数据再进行导出")
            return
        
        if not st.session_state.last_query_success:
            st.warning("⚠️ 上次查询没有成功，请重新预览数据")
            return
        
        # 跳转到导出标签页
        st.success("✅ 数据已准备好，请在「数据导出」标签页中下载")
        self.log_message("数据导出功能已就绪")
    
    def clear_filter_conditions(self):
        """清空筛选条件"""
        # 重置筛选条件
        if 'region_select' in st.session_state:
            st.session_state.region_select = ""
        if 'redeem_site' in st.session_state:
            st.session_state.redeem_site = ""
        
        st.session_state.preview_data = None
        st.session_state.last_query_success = False
        
        st.success("✅ 筛选条件已清空")
        self.log_message("筛选条件已清空")
    
    def clear_all_conditions(self):
        """清空所有条件"""
        st.session_state.selected_play_methods.clear()
        st.session_state.prize_conditions.clear()
        st.session_state.preview_data = None
        st.session_state.last_query_success = False
        
        # 重置UI状态
        if 'region_select' in st.session_state:
            st.session_state.region_select = ""
        if 'redeem_site' in st.session_state:
            st.session_state.redeem_site = ""
        if 'method_select' in st.session_state:
            st.session_state.method_select = ""
        
        st.sidebar.success("✅ 所有条件已清空")
        self.log_message("所有条件已清空")
        st.rerun()
    
    def show_statistics(self):
        """显示统计信息"""
        if st.session_state.preview_data is not None and not st.session_state.preview_data.empty:
            st.subheader("📊 数据统计")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("总记录数", len(st.session_state.preview_data))
            with col2:
                st.metric("数据列数", len(st.session_state.preview_data.columns))
            with col3:
                st.metric("数据类型", f"{len(st.session_state.preview_data.select_dtypes(include=['number']).columns)} 数值列")
            
        else:
            st.warning("暂无数据可统计")
    
    def log_message(self, message):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        username = st.session_state.username if st.session_state.username else "未登录用户"
        log_entry = f"[{timestamp}] [{username}] {message}"
        st.session_state.log_messages.append(log_entry)
        # 保持日志数量在合理范围内
        if len(st.session_state.log_messages) > 100:
            st.session_state.log_messages = st.session_state.log_messages[-50:]
    
    # 时间设置方法
    def set_today(self):
        today = datetime.now().date()
        st.session_state.redeem_start = today
        st.session_state.redeem_end = today
        st.session_state.sale_start = today
        st.session_state.sale_end = today
        self.log_message("时间范围已设置为今天")
        st.rerun()
    
    def set_last_7_days(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为最近7天")
        st.rerun()
    
    def set_last_30_days(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为最近30天")
        st.rerun()
    
    def set_this_month(self):
        today = datetime.now().date()
        start_date = today.replace(day=1)
        if today.month == 12:
            end_date = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为本月")
        st.rerun()
    
    def set_last_month(self):
        today = datetime.now().date()
        if today.month == 1:
            start_date = today.replace(year=today.year - 1, month=12, day=1)
            end_date = today.replace(day=1) - timedelta(days=1)
        else:
            start_date = today.replace(month=today.month - 1, day=1)
            end_date = today.replace(day=1) - timedelta(days=1)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为上个月")
        st.rerun()

# 运行应用
def main():
    app = LotteryDataExporterStreamlit()
    app.setup_ui()

if __name__ == "__main__":
    main()